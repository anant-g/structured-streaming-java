package com.processor.function;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import com.processor.model.StockBO;
import com.processor.util.Util;

public class StockMapper implements MapFunction<Row, StockBO> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3771873879006029604L;

	public StockBO call(Row arg0) throws Exception {
		return new StockBO(Util.getRowArray(arg0));
	}

}
