package com.upgrad.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SongDataPartitioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		String[] str = key.toString().split("#");
		String date = str[1];
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
