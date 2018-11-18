package com.upgrad.mapreduce;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.learn.mapreduce.TestAfterMapReduce;
import com.upgrad.mapreduce.domain.SongDetails;

public class TopHundredSongsGenerator {

	private static Logger logger = Logger.getLogger(MapReduceDriver.class);

	public static void main(String[] args) throws Exception {

		long t1 = System.currentTimeMillis();

		Map<String, List<SongDetails>> dateWiseSongWithCount = new HashMap<String, List<SongDetails>>();
		List<SongDetails> songs = new ArrayList<SongDetails>();

		logger.info("Starting the Counting process...");
		Map<String, Integer> preProcessData = new HashMap<String, Integer>();
		Map<String, Integer> songsMap = new HashMap<String, Integer>();

		String urlString = args[1].replace("//", "/").replace("s3a:", "https://s3.amazonaws.com/") + "part-r-00000";
		URL url = new URL(urlString);
		BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));
		String line;
		while ((line = reader.readLine()) != null) {
			String songData = line;
			if (songData != null && !songData.contains("null") && songData.split("\t").length == 2) {
				preProcessData.put(songData.split("\t")[0], Integer.parseInt(songData.split("\t")[1]));
			}
		}

		String urlString1 = args[1].replace("//", "/").replace("s3a:", "https://s3.amazonaws.com/") + "part-r-00001";
		URL url1 = new URL(urlString1);
		String line1;
		BufferedReader reader1 = new BufferedReader(new InputStreamReader(url1.openStream()));
		while ((line1 = reader1.readLine()) != null) {
			String songData = line1;
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
		dateWiseSongWithCount = TestAfterMapReduce.findTop100SongsDateWise(songs);
		Map<String, Double> result = SongsScoreGenerator.generateTopHundredSongs(dateWiseSongWithCount);
		
		BufferedWriter writer = new BufferedWriter(
				new FileWriter("E:\\Project\\SaavnProject\\Out2\\Top100SongResult.csv"));
		int index = 0;
		for (String dateSong : result.keySet()) {
			index++;
			writer.write(dateSong.split("#")[1] + "," + index + "," + dateSong.split("#")[0] + "\n");
			if (index == 100) {
				index = 0;
			}
		}
		writer.close();
		System.out.println("TIME TO COMPLETE - " + (System.currentTimeMillis() - t1) / 1000 + " Seconds");
		System.exit(1);
	}

}
