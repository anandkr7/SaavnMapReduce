package com.learn.mapreduce;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.upgrad.mapreduce.domain.Song;

public class TestAfterMapReduce {

	public static Map<String, List<Song>> findTop100SongsDateWise(List<Song> songsList) {

		Map<String, List<Song>> dateWiseSongList = new LinkedHashMap<String, List<Song>>();
		System.out.println("Creating the Datewise map...");

		for (Song songDetails : songsList) {
			if (songDetails.getPlayed() > 0) {
				if (dateWiseSongList.containsKey(songDetails.getDate())) {
					List<Song> songList = dateWiseSongList.get(songDetails.getDate());
					if (songList != null) {
						songList.add(songDetails);
						dateWiseSongList.put(songDetails.getDate(), songList);
					} else {
						List<Song> newSongList = new ArrayList<Song>();
						newSongList.add(songDetails);
						dateWiseSongList.put(songDetails.getDate(), newSongList);
					}
				} else {
					List<Song> newSongList = new ArrayList<Song>();
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
