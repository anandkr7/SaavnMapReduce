package com.upgrad.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.upgrad.mapreduce.domain.SongTextWritable;

/**
 * @author Anand
 * 
 *         Reducer Class for creating the date and song grouping
 *
 */
public class MapReduceReducer extends Reducer<SongTextWritable, IntWritable, SongTextWritable, IntWritable> {

	public void reduce(SongTextWritable key, Iterable<IntWritable> values, Context context) throws IOException {
		int count = 0;
		for (IntWritable val : values) {
			count = count + val.get();
		}
		try {
			context.write(key, new IntWritable(count));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}