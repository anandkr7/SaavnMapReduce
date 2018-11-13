package com.learn.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {

	private static List<SongDetails> songs = new ArrayList<SongDetails>();

	public static void main(String[] args) throws Exception {
		System.out.println("Starting the MapReduce program...");
		int returnStatus = ToolRunner.run(new Configuration(), new WordCount(), args);
		if (returnStatus == 0) {
			System.out.println("Exitting");
			FileHandler fileHandler = new FileHandler();
			File file = fileHandler.getFileFromExternalPath("/home/anand/Programs/Cloudera/output5/part-r-00000");

			@SuppressWarnings("resource")
			Scanner scanner = new Scanner(file);
			List<String> latestBinsDataList = new ArrayList<String>();
			Map<String, Integer> songsMap = new HashMap<String, Integer>();

			while (scanner.hasNext()) {
				String binData = scanner.nextLine();
				latestBinsDataList.add(binData);
				songsMap.put(binData.split("\t")[0], Integer.parseInt(binData.split("\t")[1]));
			}

			songsMap = TestAfterMapReduce.sortByValue(songsMap);
			for (String string : songsMap.keySet()) {
				SongDetails song = new SongDetails();
				song.setSongId(string.split("##")[0]);
				song.setDate(string.split("##")[2]);
				song.setHour(string.split("##")[1]);
				songs.add(song);
			}

			TestAfterMapReduce.findTop100SongsDateWise(songs);
		} else {
		}
	}

	public int run(String[] args) throws IOException {

		Job job = new Job(getConf());
		job.setJobName("Word Count");
		job.setJarByClass(WordCount.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);

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