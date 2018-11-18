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

}
