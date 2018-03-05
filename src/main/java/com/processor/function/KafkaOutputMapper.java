package com.processor.function;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import com.processor.model.KafkaOutputBO;

public class KafkaOutputMapper implements MapFunction<Row, KafkaOutputBO> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3771873879006029604L;

	public KafkaOutputBO call(Row arg0) throws Exception {
		return new KafkaOutputBO(arg0);
	}

}
