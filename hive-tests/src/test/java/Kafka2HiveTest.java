/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.planner.runtime.utils.TableEnvUtil;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.KafkaContainer;
import utils.Utils;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class Kafka2HiveTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(Kafka2HiveTest.class);

	@Rule
	public KafkaContainer kafka = new KafkaContainer(); // 3.2.0, 3.3.0, default is latest

	@Rule
	public DockerComposeContainer environment =
			new DockerComposeContainer("hive", new File(getClass()
					.getClassLoader()
					.getResource("docker-compose.yml").getFile()))
					.withExposedService("namenode", 8020)
					.withExposedService("hive-metastore", 9083)
					.withExposedService("hive-server", 10000)
					.withExposedService("hive-metastore-postgresql", 5432);

	@Before
	public void setUp() throws Exception {
		// Create Hive table.
		Class.forName("org.apache.hive.jdbc.HiveDriver");
		Connection con = DriverManager.getConnection(
				"jdbc:hive2://localhost:10000",
				null, null);
		Statement stmt = con.createStatement();
		stmt.execute("create table hive_table (\n" +
				"  a varchar(50),\n" +
				"  b varchar(50),\n" +
				"  c bigint,\n" +
				"  d decimal(10, 2)\n" +
				")");
		con.close();
	}

	private String factoryIdentifier() {
		return "kafka";
	}

	@Test
	public void testKafka2Hive() throws Exception {
		final String topic = "topic1";

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(
				env,
				EnvironmentSettings.newInstance()
						// Watermark is only supported in blink planner
						.useBlinkPlanner()
						.inStreamingMode()
						.build()
		);
		env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
		env.setParallelism(4);

		// register hive catalog
		final String hiveConfDir = getClass()
				.getClassLoader()
				.getResource("").getFile();
		HiveCatalog hiveCatalog = new HiveCatalog(
				"hive",
				"default",
				hiveConfDir,
				"2.3.2");
		tEnv.registerCatalog("hive", hiveCatalog);

		// ---------- Produce an event time stream into Kafka -------------------
		String groupId = "my-group";
		String bootstraps = kafka.getBootstrapServers();

		final String createKafkaTable = String.format(
				"create table kafka (\n" +
						"  `computed-price` as price + 1.0,\n" +
						"  price decimal(38, 18),\n" +
						"  currency string,\n" +
						"  log_ts timestamp(3),\n" +
						"  ts as log_ts + INTERVAL '1' SECOND,\n" +
						"  watermark for ts as ts\n" +
						") with (\n" +
						"  'connector' = '%s',\n" +
						"  'topic' = '%s',\n" +
						"  'properties.bootstrap.servers' = '%s',\n" +
						"  'properties.group.id' = '%s',\n" +
						"  'scan.startup.mode' = 'earliest-offset',\n" +
						"  'format' = 'json'\n" +
						")",
				factoryIdentifier(),
				topic,
				bootstraps,
				groupId);
		tEnv.executeSql(createKafkaTable);

		String initialValues = "INSERT INTO kafka\n" +
				"SELECT CAST(price AS DECIMAL(10, 2)), currency, CAST(ts AS TIMESTAMP(3))\n" +
				"FROM (VALUES (2.02,'Euro','2019-12-12 00:00:00.001001'), \n" +
				"  (1.11,'US Dollar','2019-12-12 00:00:01.002001'), \n" +
				"  (50,'Yen','2019-12-12 00:00:03.004001'), \n" +
				"  (3.1,'Euro','2019-12-12 00:00:04.005001'), \n" +
				"  (5.33,'US Dollar','2019-12-12 00:00:05.006001'), \n" +
				"  (0,'DUMMY','2019-12-12 00:00:10'))\n" +
				"  AS orders (price, currency, ts)";

		TableEnvUtil.execInsertSqlAndWaitResult(tEnv, initialValues);

		// ---------- Insert into Hive and consume from it. -------------------

		String insertIntoHive = "INSERT INTO hive.`default`.hive_table SELECT\n" +
				"  CAST(TUMBLE_END(ts, INTERVAL '5' SECOND) AS VARCHAR),\n" +
				"  CAST(MAX(ts) AS VARCHAR),\n" +
				"  COUNT(*),\n" +
				"  CAST(MAX(price) AS DECIMAL(10, 2))\n" +
				"FROM kafka\n" +
				"GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)";

		// TODO: 数据无法写到 hive 表 没有异常
		Utils.executeInsertAndExit(tEnv, insertIntoHive, 50000000);

		String selectFromHive = "select * from hive.`default`.hive_table";

		// Hive table is bounded, there is no need to cancel the job.
		List<String> results = Utils.executeSelectAndExit(tEnv, selectFromHive, 2, false);

		List<String> expected = Arrays.asList(
				"2019-12-12 00:00:05.000,2019-12-12 00:00:04.004,3,50.00",
				"2019-12-12 00:00:10.000,2019-12-12 00:00:06.006,2,5.33");

		assertEquals(expected, results);
	}
}
