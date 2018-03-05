package com.processor.util;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.spark.sql.Row;

public class Util {

	private static final String DATE_FORMAT = "yyyy-mm-dd hh:mm:ss";

	/**
	 * convert string into timestamp
	 * 
	 * @param str
	 * @return Timestamp
	 */
	public static Timestamp getTimeStamp(String str) {
		SimpleDateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
		Date date;
		try {
			date = formatter.parse(str);
			return new Timestamp(date.getTime());
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}

	}

	/**
	 * extract only Hour in 12 hour format from string timestamp
	 * 
	 * @param str
	 * @return String
	 */
	public static String getHourFromTimeStamp(String str) {
		try {
			Date date = new SimpleDateFormat(DATE_FORMAT).parse(str);

			return new SimpleDateFormat("H").format(date);
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}

	}

	/**
	 * array from Row object
	 * 
	 * @param row
	 * @return String[]
	 */
	public static String[] getRowArray(Row row) {
		return row.toString().replaceAll("\\[|\\]", "").split(",");

	}

	/**
	 * loading properties file
	 * 
	 * @param prop
	 * @param propFileName
	 */
	public static Properties loadProperties(String propFileName, Class clazz) {
		Properties prop = new Properties();

		InputStream inputStream = clazz.getClassLoader()
				.getResourceAsStream(propFileName);

		if (inputStream != null) {
			try {
				prop.load(inputStream);

			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			System.out.println("property file '" + propFileName
					+ "' not found in the classpath");
		}
		return prop;
	}

}
