package utils;/*
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

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class Utils {

	/**
	 * Executes the given statement and exit. Breaks and return
	 * until the given number of records are fetched.
	 *
	 * @param cancelJob If true, cancels the job when the given number results are fetched
	 *
	 * @return results as a string list
	 */
	public static List<String> executeSelectAndExit(
			StreamTableEnvironment tEnv,
			String statement,
			int expectSize,
			boolean cancelJob) {
		Preconditions.checkArgument(expectSize > 0);
		TableResult result = tEnv.executeSql(statement);
		Iterator<Row> itr = result.collect();
		List<String> results = new ArrayList<>();
		while (itr.hasNext()) {
			results.add(itr.next().toString());
			if (expectSize == results.size()) {
				if (cancelJob) {
					result.getJobClient().map(JobClient::cancel);
				}
				break;
			}
		}
		return results;
	}

	/**
	 * Executes the given statement and exit. Sleeps until timeout to break and return.
	 */
	public static void executeInsertAndExit(
			StreamTableEnvironment tEnv,
			String statement,
			long timeout) throws InterruptedException {
		Preconditions.checkArgument(timeout > 0);
		TableResult result = tEnv.executeSql(statement);
		Thread.sleep(timeout);
//		try {
//			result.getJobClient().map(JobClient::cancel);
//		} catch (Throwable var1) {
//			// Ignore.
//		}
	}
}
