package com.learn.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Logger;

public class SongDataPartitioner extends Partitioner<Text, IntWritable> {

	private static Logger logger = Logger.getLogger(WordCount.class);

	@Override
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		String[] str = key.toString().split("#");
		String date = str[1];

		try {
			String dayStr = date.substring(8, 10);
			int day = Integer.parseInt(dayStr);
			logger.info("Partition Key - " + day);
			
			if (day < 25) {
				return 1;
			}
			if (day == 25) {
				return 2;
			} else if (day == 26) {
				return 3;
			} else if (day == 27) {
				return 4;
			} else if (day == 28) {
				return 5;
			} else if (day == 29) {
				return 6;
			} else if (day == 30) {
				return 7;
			} else if (day == 31) {
				return 8;
			}

			
		} catch (Exception ex) {
			return 1;
		}
		
		return numReduceTasks;
	}

	public static void main(String[] args) {
		String date = "2017-12-01";
		String dayStr = date.substring(8, 10);
		System.out.println(dayStr);
		int day = Integer.parseInt(dayStr);
		System.out.println(day);
	}
}
