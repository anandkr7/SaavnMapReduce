package com.learn.mapreduce;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.List;
import java.util.Map;

import com.upgrad.mapreduce.SongsScoreGenerator;
import com.upgrad.mapreduce.domain.Song;

public class WordCountApp {

	public static void main(String[] args) throws Exception {

		long t1 = System.currentTimeMillis();
		Map<String, List<Song>> dateWiseSongWithCount = WordCount.startWordCount(args);

		Map<String, Double> result = SongsScoreGenerator.generateTopHundredSongs(dateWiseSongWithCount);
		BufferedWriter writer = new BufferedWriter(
				new FileWriter(args[1] + "FinalOut44Gb.txt"));
		int index = 0;
		for (String dateSong : result.keySet()) {
			index++;
			System.out.println(dateSong.split("#")[1] + "," + index + "," + dateSong.split("#")[0]);
			writer.write(dateSong.split("#")[1] + "," + index + "," + dateSong.split("#")[0] + "\n");
			if (index == 100) {
				index = 0;
			}
		}
		writer.close();
		System.out.println("TIME TO COMPLETE - " + (System.currentTimeMillis() - t1)/1000);
	}

}
