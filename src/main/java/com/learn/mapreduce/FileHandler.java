package com.learn.mapreduce;

import java.io.File;

public class FileHandler {

	public File getFile(String fileName) {

		ClassLoader classLoader = getClass().getClassLoader();
		File file = new File(classLoader.getResource(fileName).getFile());
		return file;
	}

	public File getFileFromExternalPath(String filePath) {

		File file = new File(filePath);
		return file;
	}

}
