package com.learn.mapreduce;

public class WordCountApp {

	public static void main(String[] args) throws Exception {
		
		System.out.println("Starting the MapReduce program...");
		WordCount.startWordCount(args);
		
	}
	
}
