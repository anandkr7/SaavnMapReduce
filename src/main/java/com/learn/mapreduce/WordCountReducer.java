package com.learn.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException {
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