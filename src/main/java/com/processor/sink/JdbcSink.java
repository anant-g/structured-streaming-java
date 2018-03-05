package com.processor.sink;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;

import com.processor.util.Util;

public class JdbcSink extends ForeachWriter<Row> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -873764205562254833L;

	public static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";
	public static String url;
	public static String user;
	public static String password;
	private PreparedStatement statement;
	private Connection connection;
	private Properties prop;

	public JdbcSink() {
		loadProperties();
	}

	public boolean open(long arg0, long arg1) {
		try {
			Class.forName(DRIVER_CLASS);
			connection = DriverManager.getConnection(url, user, password);
			statement = connection.prepareStatement(
					"replace into " + prop.getProperty("tableName")
							+ "(Hour,Stock,Min,Max,Volume) values(?,?,?,?,?)");
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public void loadProperties() {
		this.prop = new Properties();
		String propFileName = "config.properties";
		InputStream inputStream = this.getClass().getClassLoader()
				.getResourceAsStream(propFileName);

		if (inputStream != null) {
			try {
				prop.load(inputStream);
				user = prop.getProperty("user");
				url = prop.getProperty("url");
				password = prop.getProperty("password");

			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			System.out.println("property file '" + propFileName
					+ "' not found in the classpath");
		}
	}

	public void process(Row row) {
		String[] arr = Util.getRowArray(row);

		try {
			statement.setString(1, Util.getHourFromTimeStamp(arr[1]));
			statement.setString(2, arr[0]);
			statement.setString(3, arr[4]);
			statement.setString(4, arr[3]);
			statement.setString(5, arr[5]);
			statement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void close(Throwable arg0) {
		try {
			statement.close();
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

}
