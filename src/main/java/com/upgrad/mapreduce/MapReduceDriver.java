package com.upgrad.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.upgrad.mapreduce.domain.SongTextWritable;

public class MapReduceDriver extends Configured implements Tool {

	private static Logger logger = Logger.getLogger(MapReduceDriver.class);

	public static void main(String[] args) throws Exception {

		logger.info("Starting the MapReduce program...");
		int returnStatus = ToolRunner.run(new Configuration(), new MapReduceDriver(), args);
		System.exit(returnStatus);
		logger.info("End of MapReduce program...");
	}

	public int run(String[] args) throws IOException {

		Job job = new Job(getConf());
		job.setJobName("MapReduce");
		job.setJarByClass(MapReduceDriver.class);
		job.setOutputKeyClass(SongTextWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapOutputKeyClass(SongTextWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setMapperClass(MapReduceMapper.class);
		job.setPartitionerClass(SongDataPartitioner.class);
		job.setReducerClass(MapReduceReducer.class);
		job.setNumReduceTasks(2);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		try {
			return job.waitForCompletion(true) ? 0 : 1;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return 0;

	}

}