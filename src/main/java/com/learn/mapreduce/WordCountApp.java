package com.learn.mapreduce;

import java.util.List;
import java.util.Map;

public class WordCountApp {

	public static void main(String[] args) throws Exception {
		
		System.out.println("Starting the MapReduce program...");
		Map<String, List<SongDetails>> dateWiseSongWithCount = WordCount.startWordCount(args);
		
		TopSongsGenerator.generateTopHundredSongs(dateWiseSongWithCount);
		
	}
	
}
