package com.upgrad.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.upgrad.mapreduce.domain.SongTextWritable;

public class SongDataPartitioner extends Partitioner<SongTextWritable, IntWritable> {

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
