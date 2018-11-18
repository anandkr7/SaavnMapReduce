package com.upgrad.mapreduce;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.upgrad.mapreduce.domain.Song;

public class SongsScoreGenerator {
	private static Logger logger = Logger.getLogger(SongsScoreGenerator.class);
	public static Map<String, List<Song>> generateSongsAndDateWiseMap(List<Song> songsList) {

		Map<String, List<Song>> dateWiseSongList = new LinkedHashMap<String, List<Song>>();
		logger.info("Creating the Datewise map...");

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

	public static HashMap<String, Integer> sortByValue(Map<String, Integer> songsMap) {
		List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(songsMap.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
				return (o1.getValue()).compareTo(o2.getValue());
			}
		});

		HashMap<String, Integer> temp = new LinkedHashMap<String, Integer>();
		for (Map.Entry<String, Integer> aa : list) {
			temp.put(aa.getKey(), aa.getValue());
		}
		return temp;
	}

	public static Map<String, Double> generateTopHundredSongs(Map<String, List<Song>> dateWiseSongWithCount) {

		System.out.println("Starting the generateTopHundredSongs process...");

		Map<String, Integer> dateWiseMeanValue = new HashMap<String, Integer>();
		dateWiseMeanValue = getMeanValueForDate(dateWiseSongWithCount);

		Map<String, Double> dateWiseSDValue = new HashMap<String, Double>();
		dateWiseSDValue = getSDValueForDate(dateWiseSongWithCount, dateWiseMeanValue);

		Map<String, List<Song>> dateWiseSongWithZScore = calculateZScoreFor(dateWiseSongWithCount, dateWiseMeanValue,
				dateWiseSDValue);
		Map<String, Double> result = new LinkedHashMap<String, Double>();
		for (String dateStr : dateWiseSongWithZScore.keySet()) {
			List<Song> songs = dateWiseSongWithZScore.get(dateStr);
			List<Song> topHundredSongs = null;
			if (songs.size() > 100) {
				topHundredSongs = songs.subList(songs.size() - 101, songs.size() - 1);
			} else {
				topHundredSongs = songs;
			}
			Collections.reverse(topHundredSongs);
			for (Song songDetails : topHundredSongs) {
				result.put(dateStr + "#" + songDetails.getSongId(), songDetails.getzScore());
			}
		}
		return result;
	}

	public static Map<String, Integer> getMeanValueForDate(Map<String, List<Song>> dateWiseSongWithCount) {

		Map<String, Integer> dateWiseMeanValue = new HashMap<String, Integer>();

		for (String dateStr : dateWiseSongWithCount.keySet()) {
			List<Song> songs = dateWiseSongWithCount.get(dateStr);
			Integer totalPlayed = 0;
			int noOfSongs = songs.size();

			for (Song songDetails : songs) {
				totalPlayed += songDetails.getPlayed();
			}

			Integer meanValue = totalPlayed / noOfSongs;
			dateWiseMeanValue.put(dateStr, meanValue);
		}
		return dateWiseMeanValue;
	}

	public static Map<String, Double> getSDValueForDate(Map<String, List<Song>> dateWiseSongWithCount,
			Map<String, Integer> dateWiseMeanValue) {

		Map<String, Double> dateSDMeanValue = new HashMap<String, Double>();

		for (String dateStr : dateWiseSongWithCount.keySet()) {
			List<Song> songs = dateWiseSongWithCount.get(dateStr);
			BigInteger totalSquareValue = BigInteger.valueOf(0);
			int noOfSongs = songs.size();
			for (Song songDetails : songs) {

				Integer mean = dateWiseMeanValue.get(dateStr);
				Integer val = (songDetails.getPlayed() - mean);

				BigInteger valSquare = BigInteger.valueOf(val * val);
				totalSquareValue = totalSquareValue.add(valSquare);
			}

			logger.info("Date - " + dateStr + " Size - " + noOfSongs);
			BigInteger dividedVal = totalSquareValue.divide(BigInteger.valueOf(noOfSongs - 1));
			BigInteger temp = new BigInteger(sqrt(dividedVal).toString());
			dateSDMeanValue.put(dateStr, temp.doubleValue());
		}

		return dateSDMeanValue;
	}

	public static Map<String, List<Song>> calculateZScoreFor(Map<String, List<Song>> dateWiseSongWithCount,
			Map<String, Integer> dateWiseMeanValue, Map<String, Double> dateSDMeanValue) {

		Map<String, List<Song>> updatedDateWiseSongWithCount = new HashMap<String, List<Song>>();
		for (String dateStr : dateWiseSongWithCount.keySet()) {
			List<Song> songs = dateWiseSongWithCount.get(dateStr);
			List<Song> upDatedSongs = new ArrayList<Song>();
			for (Song songDetails : songs) {
				Double sd = dateSDMeanValue.get(dateStr);
				Integer mean = dateWiseMeanValue.get(dateStr);
				int palyed = songDetails.getPlayed();

				double zs = (Integer.valueOf(palyed) - mean) / sd;
				songDetails.setzScore(zs);
				upDatedSongs.add(songDetails);

			}
			updatedDateWiseSongWithCount.put(dateStr, upDatedSongs);
		}

		return updatedDateWiseSongWithCount;
	}

	static BigInteger sqrt(BigInteger n) {
		BigInteger a = BigInteger.ONE;
		BigInteger b = new BigInteger(n.shiftRight(5).add(new BigInteger("8")).toString());
		while (b.compareTo(a) >= 0) {
			BigInteger mid = new BigInteger(a.add(b).shiftRight(1).toString());
			if (mid.multiply(mid).compareTo(n) > 0)
				b = mid.subtract(BigInteger.ONE);
			else
				a = mid.add(BigInteger.ONE);
		}
		return a.subtract(BigInteger.ONE);
	}

}
