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

	private static int REQUEST_TIME_INDEX = 0;
	private static int REQUEST_ELAPSE_TIME_INDEX = 1;
	private static int REQUEST_LABEL_INDEX = 2;
	private static int REQUEST_SUCCESSFUL_INDEX = 7;
	private static int REQUEST_BYTE_INDEX = 8;
	private static int INTERVAL_TIME = 120;
	
	private static final String SEPARATOR = ",";
	private static final int OTHER_ARGS_SIZE = 4;

	/**
	 * Counts the words in each line. For each line of input, break the line
	 * into words and emit them as (<b>word</b>, <b>1</b>).
	 */
	public static class MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString().trim();

			if (line.length() > 5) {
				String [] fields = line.split(SEPARATOR);
				String label = Long.parseLong(fields[REQUEST_TIME_INDEX]) / (INTERVAL_TIME * 1000)
						+ SEPARATOR + fields[REQUEST_LABEL_INDEX] + SEPARATOR
						+ fields[REQUEST_SUCCESSFUL_INDEX];
				String content = fields[REQUEST_ELAPSE_TIME_INDEX] + SEPARATOR 
				        + fields[REQUEST_BYTE_INDEX];
				output.collect(new Text(label), new Text(content));
			}
		}
	}

	/**
	 * A reducer class that just emits the sum of the input values.
	 */
	public static class Combine extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			long sumElapse = 0L;
			long sumByte = 0L;
			long count = 0L;
			while (values.hasNext()) {
				String [] fields = values.next().toString().split(SEPARATOR);
				sumElapse += Long.parseLong(fields[0]);
				sumByte += Long.parseLong(fields[1]);
				count++;
			}
			String content = sumElapse + SEPARATOR + sumByte + SEPARATOR + count;
			output.collect(key, new Text(content));
		}
	}

	/**
	 * A reducer class that just emits the sum of the input values.
	 */
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			long sumElapse = 0L;
			long sumByte = 0L;
			long count = 0L;
			while (values.hasNext()) {
				String[] fields = values.next().toString().split(SEPARATOR);
				sumElapse += Long.parseLong(fields[0]);
				sumByte += Long.parseLong(fields[1]);
				count += Long.parseLong(fields[2]);
			}
			String content = sumElapse + SEPARATOR + count + SEPARATOR
					+ sumElapse / count + SEPARATOR + sumByte / count;
			output.collect(key, new Text(content));
		}
	}

	static int printUsage() {
		System.out
				.println("wordcount [-m <maps>] [-r <reduces>] <input> <output>");
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

		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			try {
				if ("-m".equals(args[i])) {
					conf.setNumMapTasks(Integer.parseInt(args[++i]));
				} else if ("-r".equals(args[i])) {
					conf.setNumReduceTasks(Integer.parseInt(args[++i]));
				} else {
					other_args.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of "
						+ args[i]);
				return printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from "
						+ args[i - 1]);
				return printUsage();
			}
		}
		// Make sure there are exactly 4 parameters left.
		if (other_args.size() != OTHER_ARGS_SIZE) {
			System.out.println("ERROR: Wrong number of parameters: "
					+ other_args.size() + " instead of 2.");
			return printUsage();
		}
		
		String [] fields = other_args.get(2).split(SEPARATOR);
		for(int i = 0; i < fields.length; i++) {
			if ("timeStamp".equals(fields[i])) {
				REQUEST_TIME_INDEX = i;
			} else if("elapsed".equals(fields[i])) {
				REQUEST_ELAPSE_TIME_INDEX = i;
			} else if("label".equals(fields[i])) {
				REQUEST_LABEL_INDEX = i;
			} else if("success".equals(fields[i])) {
				REQUEST_SUCCESSFUL_INDEX = i;
			} else if("bytes".equals(fields[i])) {
				REQUEST_BYTE_INDEX = i;
			}
		}
		
		INTERVAL_TIME = Integer.parseInt(other_args.get(3));
		
		FileInputFormat.setInputPaths(conf, other_args.get(0));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new AverageResponseTime(),
				args);
		System.exit(res);
	}

}
