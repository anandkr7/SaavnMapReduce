package com.upgrad.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.upgrad.mapreduce.domain.SongTextWritable;

public class MapReduceMapper extends Mapper<LongWritable, Text, SongTextWritable, IntWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		SongTextWritable song = new SongTextWritable();
		
		String st[] = value.toString().split(",");
		String songId = st[0].trim();
		String date = st[4].trim();
		
		song.setSongId(new Text(songId));
		song.setDate(new Text(date));
		context.write(song, new IntWritable(1));
		
	}
}
