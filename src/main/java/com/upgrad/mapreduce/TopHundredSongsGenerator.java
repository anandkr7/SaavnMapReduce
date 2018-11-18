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

import com.upgrad.mapreduce.domain.Song;
import com.upgrad.mapreduce.util.CommonUtils;

/**
 * @author Anand
 * 
 *         Processor Main Class for processing the MapReduce output and generate
 *         the Top hundred songs
 *
 */
public class TopHundredSongsGenerator {

	private static Logger logger = Logger.getLogger(TopHundredSongsGenerator.class);

	// Method to Start the Mean, SD, ZScore calculation and generation of the Top
	// Hundred songs
	public static void main(String[] args) throws Exception {

		long t1 = System.currentTimeMillis();

		Map<String, List<Song>> dateWiseSongWithCount = new HashMap<String, List<Song>>();
		List<Song> songs = new ArrayList<Song>();

		logger.info("Starting the Counting process...");
		Map<String, Integer> songsMap = new HashMap<String, Integer>();

		String urlString1 = args[0].replace("//", "/").replace("s3a:", "https://s3.amazonaws.com/") + "part-r-00001";
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
		songsMap = SongsScoreGenerator.sortByValue(songsMap);
		for (String string : songsMap.keySet()) {
			Song song = new Song();
			song.setSongId(string.split(",")[0]);
			song.setDate(string.split(",")[1]);
			song.setPlayed(songsMap.get(string));

			if (CommonUtils.isValidDate(song.getDate()))
				songs.add(song);
		}

		logger.info("Generating the Song And Date wise map...");
		dateWiseSongWithCount = SongsScoreGenerator.generateSongsAndDateWiseMap(songs);

		logger.info("Generating the Top Hundred Songs by using ZScore...");
		Map<String, Double> result = SongsScoreGenerator.generateTopHundredSongs(dateWiseSongWithCount);

		logger.info("Starting to write to the files...");
		BufferedWriter writer25 = new BufferedWriter(new FileWriter(args[1] + "25.txt"));
		BufferedWriter writer26 = new BufferedWriter(new FileWriter(args[1] + "26.txt"));
		BufferedWriter writer27 = new BufferedWriter(new FileWriter(args[1] + "27.txt"));
		BufferedWriter writer28 = new BufferedWriter(new FileWriter(args[1] + "28.txt"));
		BufferedWriter writer29 = new BufferedWriter(new FileWriter(args[1] + "29.txt"));
		BufferedWriter writer30 = new BufferedWriter(new FileWriter(args[1] + "30.txt"));
		BufferedWriter writer31 = new BufferedWriter(new FileWriter(args[1] + "31.txt"));

		int index = 0;
		BufferedWriter writer = null;
		for (String dateSong : result.keySet()) {
			index++;

			if (dateSong.split("#")[0].contains("2017-12-25")) {
				writer = writer25;
			} else if (dateSong.split("#")[0].contains("2017-12-26")) {
				writer = writer26;
			} else if (dateSong.split("#")[0].contains("2017-12-27")) {
				writer = writer27;
			} else if (dateSong.split("#")[0].contains("2017-12-28")) {
				writer = writer28;
			} else if (dateSong.split("#")[0].contains("2017-12-29")) {
				writer = writer29;
			} else if (dateSong.split("#")[0].contains("2017-12-30")) {
				writer = writer30;
			} else if (dateSong.split("#")[0].contains("2017-12-31")) {
				writer = writer31;
			}

			writer.write(dateSong.split("#")[1] + "," + index + "," + dateSong.split("#")[0] + "\n");
			if (index == 100) {
				index = 0;
			}
		}
		writer25.close();
		writer26.close();
		writer27.close();
		writer28.close();
		writer29.close();
		writer30.close();
		writer31.close();
		logger.info("TIME TO COMPLETE - " + (System.currentTimeMillis() - t1) / 1000 + " Seconds");
		System.exit(1);
	}

}
