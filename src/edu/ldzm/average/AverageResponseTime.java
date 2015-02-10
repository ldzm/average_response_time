/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.ldzm.average;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This is an example Hadoop Map/Reduce application. It reads the text input
 * files, breaks each line into words and counts them. The output is a locally
 * sorted list of words and the count of how often they occurred.
 * 
 * To run: bin/hadoop jar build/hadoop-examples.jar wordcount [-m <i>maps</i>]
 * [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i>
 */
public class AverageResponseTime extends Configured implements Tool {

	private static final String SEPARATOR = ",";

	/**
	 * Counts the words in each line. For each line of input, break the line
	 * into words and emit them as (<b>word</b>, <b>1</b>).
	 */
	public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		private Configuration conf;
		
		@Override
		public void configure(JobConf job) {
			conf = job;
		}
		/**
		 * output key: time,label content: elapse,byte,true/false
		 */
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			int REQUEST_TIME_INDEX = conf.getInt("REQUEST_TIME_INDEX", -1);
			int REQUEST_ELAPSE_TIME_INDEX = conf.getInt("REQUEST_ELAPSE_TIME_INDEX", -1);
			int REQUEST_LABEL_INDEX = conf.getInt("REQUEST_LABEL_INDEX", -1);
			int REQUEST_SUCCESSFUL_INDEX = conf.getInt("REQUEST_SUCCESSFUL_INDEX", -1);
			int REQUEST_BYTE_INDEX = conf.getInt("REQUEST_BYTE_INDEX", -1);
			int INTERVAL_TIME = conf.getInt("INTERVAL_TIME", -1);
			int NAME_LIST_LENGTH = conf.getInt("NAME_LIST_LENGTH", -1);
			
			String[] fields = value.toString().trim().split(SEPARATOR);
			if (fields.length == NAME_LIST_LENGTH) {

				String label = Long.parseLong(fields[REQUEST_TIME_INDEX]) / (INTERVAL_TIME * 1000) + SEPARATOR
						+ fields[REQUEST_LABEL_INDEX];

				String content = fields[REQUEST_ELAPSE_TIME_INDEX] + SEPARATOR + fields[REQUEST_BYTE_INDEX] + SEPARATOR
						+ fields[REQUEST_SUCCESSFUL_INDEX];
				output.collect(new Text(label), new Text(content));
			}
		}
	}

	/**
	 * A reducer class that just emits the sum of the input values. output key:
	 * time,label content:
	 * sumSuccessElapse,sumFailureElapse,sumSuccessByte,sumFailureByte
	 * ,successCount,failureCount;
	 */
	public static class Combine extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			long sumSuccessElapse = 0L;
			long sumFailureElapse = 0L;
			long sumSuccessByte = 0L;
			long sumFailureByte = 0L;
			long successCount = 0L;
			long failureCount = 0L;
			while (values.hasNext()) {
				String[] fields = values.next().toString().split(SEPARATOR);

				if ("true".equals(fields[2])) {
					sumSuccessElapse += Long.parseLong(fields[0]);
					sumSuccessByte += Long.parseLong(fields[1]);
					successCount++;
				} else {
					sumFailureElapse += Long.parseLong(fields[0]);
					sumFailureByte += Long.parseLong(fields[1]);
					failureCount++;
				}
			}
			String content = sumSuccessElapse + SEPARATOR + sumFailureElapse + SEPARATOR + sumSuccessByte + SEPARATOR
					+ sumFailureByte + SEPARATOR + successCount + SEPARATOR + failureCount;
			output.collect(key, new Text(content));
		}
	}

	/**
	 * A reducer class that just emits the sum of the input values. 
	 * output: key:time,label
	 * content: sumSuccessElapse,sumSuccessElapse/successCount,sumFailureElapse,sumFailureElapse/failureCount,sumSuccessByte,
	 * sumSuccessByte/successCount,sumFailureByte,sumFailureByte/failureCount,successCount,failureCount;
	 */
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        private Configuration conf; 
        
        @Override
        public void configure(JobConf job) {
        	conf = job;
        }
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			
			int INTERVAL_TIME = conf.getInt("INTERVAL_TIME", -1);
			
			long sumSuccessElapse = 0L;
			long sumFailureElapse = 0L;
			long sumSuccessByte = 0L;
			long sumFailureByte = 0L;
			long successCount = 0L;
			long failureCount = 0L;
			while (values.hasNext()) {
				String[] fields = values.next().toString().split(SEPARATOR);
				sumSuccessElapse += Long.parseLong(fields[0]);
				sumFailureElapse += Long.parseLong(fields[1]);
				sumSuccessByte += Long.parseLong(fields[2]);
				sumFailureByte += Long.parseLong(fields[3]);
				successCount += Long.parseLong(fields[4]);
				failureCount += Long.parseLong(fields[5]);
			}
			if (0 == successCount) {
				successCount = -1L;
			}
			if (0 == failureCount) {
				failureCount = -1L;
			}
			DecimalFormat df = new DecimalFormat("##0.000");
			String content = sumSuccessElapse + SEPARATOR + df.format(sumSuccessElapse * 1.0 / successCount) + SEPARATOR
					+ sumFailureElapse + SEPARATOR + df.format(sumFailureElapse * 1.0 / failureCount) + SEPARATOR + sumSuccessByte
					+ SEPARATOR + df.format(sumSuccessByte * 1.0 / (INTERVAL_TIME * 1000)) + SEPARATOR + sumFailureByte + SEPARATOR
					+ df.format(sumFailureByte * 1.0 / (INTERVAL_TIME * 1000)) + SEPARATOR + successCount + SEPARATOR + failureCount;
			output.collect(key, new Text(content));
		}
	}

	static int printUsage() {
		System.out.println("average_response_time [-m <maps>] [-r <reduces>] -l <namelist> -i <interval> <input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * The main driver for word count map/reduce program. Invoke this method to
	 * submit the map/reduce job.
	 * 
	 * @throws IOException
	 *             When there is communication problems with the job tracker.
	 */
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), AverageResponseTime.class);
		conf.setJobName("average_response_time");

		// the keys are words (strings)
		conf.setOutputKeyClass(Text.class);
		// the values are counts (ints)
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MapClass.class);
		conf.setCombinerClass(Combine.class);
		conf.setReducerClass(Reduce.class);

		int param = 0;
		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			try {
				if ("-m".equals(args[i])) {
					conf.setNumMapTasks(Integer.parseInt(args[++i]));
				} else if ("-r".equals(args[i])) {
					conf.setNumReduceTasks(Integer.parseInt(args[++i]));
				} else if ("-l".equals(args[i])) {
					param++;
					String[] fields = args[++i].split(SEPARATOR);
					conf.setInt("NAME_LIST_LENGTH", fields.length);
					for (int j = 0; j < fields.length; j++) {
						if ("timeStamp".equals(fields[j])) {
							conf.setInt("REQUEST_TIME_INDEX", j);
						} else if ("elapsed".equals(fields[j])) {
							conf.setInt("REQUEST_ELAPSE_TIME_INDEX", j);
						} else if ("label".equals(fields[j])) {
							conf.setInt("REQUEST_LABEL_INDEX", j);
						} else if ("success".equals(fields[j])) {
							conf.setInt("REQUEST_SUCCESSFUL_INDEX", j);
						} else if ("bytes".equals(fields[j])) {
							conf.setInt("REQUEST_BYTE_INDEX", j);
						}
					}
				} else if ("-i".equals(args[i])) {
					param++;
					conf.setInt("INTERVAL_TIME", Integer.parseInt(args[++i]));
				} else {
					other_args.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of " + args[i]);
				return printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from " + args[i - 1]);
				return printUsage();
			}
		}
		// Make sure there are exactly 2 parameters left.
		if (other_args.size() != 2) {
			System.out.println("ERROR: Wrong number of parameters: " + other_args.size() + " instead of 2.");
			return printUsage();
		}
		
		if (param != 2) {
			System.out.println("请输入-l 和 -i的参数。");
			return printUsage();
		}

		FileInputFormat.setInputPaths(conf, other_args.get(0));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new AverageResponseTime(), args);
		System.exit(res);
	}

}
