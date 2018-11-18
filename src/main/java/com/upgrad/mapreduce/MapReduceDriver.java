package com.upgrad.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

import com.learn.mapreduce.TestAfterMapReduce;
import com.upgrad.mapreduce.domain.SongDetails;

public class MapReduceDriver extends Configured implements Tool {

	private static List<SongDetails> songs = new ArrayList<SongDetails>();
	private static Logger logger = Logger.getLogger(MapReduceDriver.class);

	public static Map<String, List<SongDetails>> startWordCount(String[] args) throws Exception {

		logger.info("Starting the MapReduce program...");
		int returnStatus = ToolRunner.run(new Configuration(), new MapReduceDriver(), args);

		if (returnStatus == 0) {

			logger.info("Starting the Counting process...");

			Map<String, Integer> songsMap = new HashMap<String, Integer>();
			
			String urlString = args[1].replace("//", "/").replace("s3a:", "https://s3.amazonaws.com/") + "part-r-00000";
			URL url = new URL(urlString);
			BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));
			String line;
			while ((line = reader.readLine()) != null) {
				String songData = line;
				if (songData != null && !songData.contains("null") && songData.split("\t").length == 2) {
					songsMap.put(songData.split("\t")[0], Integer.parseInt(songData.split("\t")[1]));
				}
			}

			urlString = args[1].replace("//", "/").replace("s3a:", "https://s3.amazonaws.com/") + "part-r-00001";
			url = new URL(urlString);
			reader = new BufferedReader(new InputStreamReader(url.openStream()));
			while ((line = reader.readLine()) != null) {
				String songData = line;
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
		job.setJobName("MapReduceDriver");
		job.setJarByClass(MapReduceDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(MapReduceMapper.class);
		job.setCombinerClass(MapReduceReducer.class);
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