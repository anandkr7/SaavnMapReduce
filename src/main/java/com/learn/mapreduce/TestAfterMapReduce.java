package com.learn.mapreduce;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TestAfterMapReduce {

	static Map<String, List<SongDetails>> findTop100SongsDateWise(List<SongDetails> songsList) {

		Map<String, List<SongDetails>> dateWiseSongList = new LinkedHashMap<String, List<SongDetails>>();
		System.out.println("Creating the Datewise map...");

		for (SongDetails songDetails : songsList) {
			if (songDetails.getPlayed() > 100) {
				if (dateWiseSongList.containsKey(songDetails.getDate())) {
					List<SongDetails> songList = dateWiseSongList.get(songDetails.getDate());
					if (songList != null) {
						songList.add(songDetails);
						dateWiseSongList.put(songDetails.getDate(), songList);
					} else {
						List<SongDetails> newSongList = new ArrayList<SongDetails>();
						newSongList.add(songDetails);
						dateWiseSongList.put(songDetails.getDate(), newSongList);
					}
				} else {
					List<SongDetails> newSongList = new ArrayList<SongDetails>();
					newSongList.add(songDetails);
					dateWiseSongList.put(songDetails.getDate(), newSongList);
				}
			}
		}
		return dateWiseSongList;
	}

	// function to sort hashmap by values
	public static HashMap<String, Integer> sortByValue(Map<String, Integer> songsMap) {
		// Create a list from elements of HashMap
		List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(songsMap.entrySet());

		// Sort the list
		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
				return (o1.getValue()).compareTo(o2.getValue());
			}
		});

		// put data from sorted list to hashmap
		HashMap<String, Integer> temp = new LinkedHashMap<String, Integer>();
		for (Map.Entry<String, Integer> aa : list) {
			temp.put(aa.getKey(), aa.getValue());
		}
		return temp;
	}

}
