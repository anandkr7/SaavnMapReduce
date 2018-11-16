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
import org.apache.log4j.Logger;

public class WordCount extends Configured implements Tool {

	private static List<SongDetails> songs = new ArrayList<SongDetails>();
    private static Logger logger = Logger.getLogger(WordCount.class);

	public static Map<String, List<SongDetails>> startWordCount(String[] args) throws Exception {

		logger.info("Starting the MapReduce program...");
		int returnStatus = ToolRunner.run(new Configuration(), new WordCount(), args);

		//		int returnStatus = 0;
		if (returnStatus == 0) {

			logger.info("Starting the Counting process...");
			FileHandler fileHandler = new FileHandler();
			File file = fileHandler.getFileFromExternalPath(
					"E:\\Project\\Saavn_Files\\converted_44gb\\mapreduce_output_songdate_44gb.txt");
			// File file =
			// fileHandler.getFileFromExternalPath("/home/anand/Project/Pig/Saavn/Out1/part-r-00000");

			@SuppressWarnings("resource")
			Scanner scanner = new Scanner(file);
			Map<String, Integer> songsMap = new HashMap<String, Integer>();

			while (scanner.hasNext()) {
				String songData = scanner.nextLine();
				if (songData != null && !songData.contains("null") && songData.split("\t").length == 2) {
					songsMap.put(songData.split("\t")[0], Integer.parseInt(songData.split("\t")[1]));
				}
			}

			logger.info("Creating the Song objects...");
			songsMap = TestAfterMapReduce.sortByValue(songsMap);
			for (String string : songsMap.keySet()) {
				SongDetails song = new SongDetails();
				song.setSongId(string.split("#")[0]);
				song.setDate(string.split("#")[1]);
				song.setPlayed(songsMap.get(string));
				songs.add(song);
			}

			logger.info("Start finding the Song objects...");
			return TestAfterMapReduce.findTop100SongsDateWise(songs);
		} else {
		}
		return null;
	}

	public int run(String[] args) throws IOException {

		Job job = new Job(getConf());
		job.setJobName("Word Count");
		job.setJarByClass(WordCount.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(WordCountMapper.class);
		job.setPartitionerClass(SongDataPartitioner.class);
		job.setReducerClass(WordCountReducer.class);
		job.setNumReduceTasks(10);

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