package com.processor.model;

import java.io.Serializable;
import java.sql.Timestamp;

import com.processor.util.Util;

public class StockBO implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3189446487506526219L;

	private long transId;
	private Timestamp timestamp;
	private String stockCode;
	private String seller;
	private String buyer;
	private double price;
	private long volume;

	public StockBO(String[] row) {
		this.transId = Long.parseLong(row[0]);
		this.timestamp = Util.getTimeStamp(row[1]);
		this.stockCode = row[2];
		this.seller = row[3];
		this.buyer = row[4];
		this.price = Double.parseDouble(row[5]);
		this.volume = Long.parseLong(row[6]);
	}

	public long getTransId() {
		return transId;
	}

	public void setTransId(long transId) {
		this.transId = transId;
	}

	public Timestamp getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Timestamp timestamp) {
		this.timestamp = timestamp;
	}

	public String getSeller() {
		return seller;
	}

	public void setSeller(String seller) {
		this.seller = seller;
	}

	public String getBuyer() {
		return buyer;
	}

	public void setBuyer(String buyer) {
		this.buyer = buyer;
	}

	public Double getPrice() {
		return price;
	}

	public void setPrice(Double price) {
		this.price = price;
	}

	public long getVolume() {
		return volume;
	}

	public void setVolume(long volume) {
		this.volume = volume;
	}

	public String getStockCode() {
		return stockCode;
	}

	public void setStockCode(String stockCode) {
		this.stockCode = stockCode;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((stockCode == null) ? 0 : stockCode.hashCode());
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
		StockBO other = (StockBO) obj;
		if (stockCode == null) {
			if (other.stockCode != null)
				return false;
		} else if (!stockCode.equals(other.stockCode))
			return false;
		return true;
	}

}
