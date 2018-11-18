package com.upgrad.mapreduce.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class CommonUtils {

	public static boolean isValidDate(String date) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		try {
			format.parse(date);
			return true;
		} catch (ParseException e) {
			return false;
		}
	}

	public static boolean isValidSongId(String data) {
		if (data != null && !data.equalsIgnoreCase("null") && !data.contains("null")) {
			return true;
		} else {
			return false;
		}
	}
}
