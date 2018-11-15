package com.learn.mapreduce;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopSongsGenerator {

	public static Map<String, String> generateTopHundredSongs(Map<String, List<SongDetails>> dateWiseSongWithCount) {

		System.out.println("Starting the generateTopHundredSongs process...");

		Map<String, Integer> dateWiseMeanValue = new HashMap<String, Integer>();
		dateWiseMeanValue = getMeanValueForDate(dateWiseSongWithCount);
		for (String dateStr : dateWiseMeanValue.keySet()) {
			System.out.println("Date - " + dateStr + " Mean - " + dateWiseMeanValue.get(dateStr));
		}

		Map<String, Double> dateWiseSDValue = new HashMap<String, Double>();
		dateWiseSDValue = getSDValueForDate(dateWiseSongWithCount, dateWiseMeanValue);
		for (String dateStr : dateWiseSDValue.keySet()) {
			System.out.println("Date - " + dateStr + " SD - " + dateWiseSDValue.get(dateStr));
		}
		
		calculateZScoreFor(dateWiseSongWithCount, dateWiseMeanValue, dateWiseSDValue);
		return null;
	}

	public static Map<String, Integer> getMeanValueForDate(Map<String, List<SongDetails>> dateWiseSongWithCount) {

		Map<String, Integer> dateWiseMeanValue = new HashMap<String, Integer>();

		for (String dateStr : dateWiseSongWithCount.keySet()) {
			List<SongDetails> songs = dateWiseSongWithCount.get(dateStr);
			Integer totalPlayed = 0;
			int noOfSongs = songs.size();

			for (SongDetails songDetails : songs) {
				totalPlayed += songDetails.getPlayed();
			}

			Integer meanValue = totalPlayed / noOfSongs;
			dateWiseMeanValue.put(dateStr, meanValue);
		}
		return dateWiseMeanValue;
	}

	public static Map<String, Double> getSDValueForDate(Map<String, List<SongDetails>> dateWiseSongWithCount,
			Map<String, Integer> dateWiseMeanValue) {

		Map<String, Double> dateSDMeanValue = new HashMap<String, Double>();
		for (String dateStr : dateWiseSongWithCount.keySet()) {
			List<SongDetails> songs = dateWiseSongWithCount.get(dateStr);
			BigInteger totalSquareValue = BigInteger.valueOf(0);
			int noOfSongs = songs.size();

			for (SongDetails songDetails : songs) {

				Integer mean = dateWiseMeanValue.get(dateStr);
				// System.out.println("Mean -- " + mean);
				Integer val = (songDetails.getPlayed() - mean);

				BigInteger valSquare = BigInteger.valueOf(val * val);
				totalSquareValue = totalSquareValue.add(valSquare);
				// System.out.println("Song Id - " + songDetails.getSongId() +", Current Val --
				// " + val + " Square Val -- " + valSquare);
				// System.out.println("Incremental Val -- " + totalSquareValue);
			}

			// System.out.println("Total Val -- " + totalSquareValue);
			BigInteger dividedVal = totalSquareValue.divide(BigInteger.valueOf(noOfSongs - 1));
			BigInteger temp = new BigInteger(sqrt(dividedVal).toString());
			dateSDMeanValue.put(dateStr, temp.doubleValue());
		}

		return dateSDMeanValue;
	}

	public static Map<String, List<SongDetails>> calculateZScoreFor(Map<String, List<SongDetails>> dateWiseSongWithCount,
			Map<String, Integer> dateWiseMeanValue, Map<String, Double> dateSDMeanValue) {

		Map<String, List<SongDetails>> updatedDateWiseSongWithCount = new HashMap<String, List<SongDetails>>();
		for (String dateStr : dateWiseSongWithCount.keySet()) {
			System.out.println("\n\nDATE -- " + dateStr);
			List<SongDetails> songs = dateWiseSongWithCount.get(dateStr);
			List<SongDetails> upDatedSongs = new ArrayList<SongDetails>();
			for (SongDetails songDetails : songs) {
				Double sd = dateSDMeanValue.get(dateStr);
				Integer mean = dateWiseMeanValue.get(dateStr);
				int palyed = songDetails.getPlayed();

				double zs = (Integer.valueOf(palyed) - mean) / sd;
				System.out.println("SongId - " + songDetails.getSongId() + " Zscore - " + zs);
				songDetails.setzScore(zs);
				upDatedSongs.add(songDetails);
			}
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
