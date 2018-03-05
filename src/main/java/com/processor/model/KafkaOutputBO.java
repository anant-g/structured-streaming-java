package com.processor.model;

import java.io.Serializable;

import org.apache.spark.sql.Row;

import com.processor.util.Util;

public class KafkaOutputBO implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5606646447090712750L;

	private String key;
	private String value;

	public KafkaOutputBO(Row row) {
		String[] col = Util.getRowArray(row);
		this.key = col[0] + " "+ col[1] + " "+ col[2];
		this.value = Util.getHourFromTimeStamp(col[1]) + "|" + col[0] + "|"
				+ col[4] + "|" + col[3] + "|" + col[5];
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		KafkaOutputBO other = (KafkaOutputBO) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		return true;
	}

}
