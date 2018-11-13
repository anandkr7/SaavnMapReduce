package com.learn.mapreduce;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TestAfterMapReduce {

	static void findTop100SongsDateWise(List<SongDetails> songsList) {

		Map<String, List<SongDetails>> dateWiseSongList = new LinkedHashMap<String, List<SongDetails>>();

		for (SongDetails songDetails : songsList) {
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

		try {
			BufferedWriter writer = new BufferedWriter(
					new FileWriter("/home/anand/Programs/Cloudera/output5/finalOut.txt"));
			for (String dateStr : dateWiseSongList.keySet()) {
				System.out.println("Date -- " + dateStr);
				List<SongDetails> songs = dateWiseSongList.get(dateStr);
				Map<String, Integer> songsCountMap = new HashMap<String, Integer>();
				for (SongDetails songDetails : songs) {
					if (songsCountMap.containsKey(songDetails.getSongId())) {
						Integer count = songsCountMap.get(songDetails.getSongId());
						if (count != null) {
							count += songDetails.getPlayed();
							songsCountMap.put(songDetails.getSongId(), count);
						} else {
							count = songDetails.getPlayed();
							songsCountMap.put(songDetails.getSongId(), count);
						}
					} else {
						Integer count = songDetails.getPlayed();
						songsCountMap.put(songDetails.getSongId(), count);
					}
				}

				songsCountMap = sortByValue(songsCountMap);
				for (String songDetails : songsCountMap.keySet()) {
					if(songsCountMap.get(songDetails) > 1)
						writer.write(dateStr + "---" + songDetails + ", Count - " + songsCountMap.get(songDetails) + "\n");
				}
			}
			writer.close();
		} catch (Exception ex) {

		}

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
