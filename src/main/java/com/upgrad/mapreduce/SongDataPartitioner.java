package com.upgrad.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.upgrad.mapreduce.domain.SongTextWritable;

/**
 * @author Anand
 * 
 *         Patitioner Class for processing the Dec 1st to Dec 24th entries and
 *         Dec 25th to Dec 31st separately.
 *
 */
public class SongDataPartitioner extends Partitioner<SongTextWritable, IntWritable> {

	// Method to generate the partition key
	@Override
	public int getPartition(SongTextWritable key, IntWritable value, int numReduceTasks) {
		String date = key.getDate().toString();
		int partition = 0;
		try {
			String dayStr = date.substring(8, 10);
			int day = Integer.parseInt(dayStr);
			if (day < 25)
				partition = 0;
			else
				partition = 1;

		} catch (Exception ex) {
			return 0;
		}
		return partition;
	}
}
