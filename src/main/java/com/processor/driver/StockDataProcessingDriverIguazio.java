package com.processor.driver;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import com.processor.function.KafkaOutputMapper;
import com.processor.function.StockMapper;
import com.processor.model.KafkaOutputBO;
import com.processor.model.StockBO;
import com.processor.sink.JdbcSink;
import com.processor.util.Util;

public class StockDataProcessingDriverIguazio {

	private SparkSession sparkSession;
	private String outputMode;
	private Map<String, String> config = new HashMap<String, String>();
	private Properties prop;
	private String propFileName;
	private String shutdownHook;

	public StockDataProcessingDriverIguazio() {
		this.propFileName = "config.properties";
		init();
	}

	// constructor for integration tests
	public StockDataProcessingDriverIguazio(String propertyFile, String shutdownHook) {
		this.propFileName = propertyFile;
		this.shutdownHook = shutdownHook;
		init();
	}

	public void init() {

		this.prop = Util.loadProperties(propFileName, this.getClass());
		this.sparkSession = SparkSession.builder()
				.config("spark.master", this.prop.getProperty("sparkMaster"))
				.appName("market-data-app").getOrCreate();
		this.outputMode = this.prop.getProperty("outputMode");
		config.put("kafka.bootstrap.servers",
				this.prop.getProperty("kafka.bootstrap.servers.sink"));
		config.put("subscribe", this.prop.getProperty("inputTopicName"));
		config.put("maxOffsetsPerTrigger",
				this.prop.getProperty("maxOffsetsPerTrigger"));
		config.put("startingOffsets", this.prop.getProperty("startingOffsets"));
	}

	public static void main(String[] args) {
		StockDataProcessingDriverIguazio driver = new StockDataProcessingDriverIguazio();
		driver.run();

	}

	public void run() {
		Dataset<Row> df = this.sparkSession.readStream().format("kafka")
				.options(this.config).load();
		Dataset<Row> df_value = df.selectExpr("CAST(value AS STRING)");
		Dataset<StockBO> dsStocks = df_value.map(new StockMapper(),
				Encoders.bean(StockBO.class));

		

		Dataset<Row> aggResult = dsStocks
				.groupBy(dsStocks.col("stockCode").alias("Stock"),
						functions.window(dsStocks.col("timestamp"), "1 hour")
								.alias("Hour"))
				.agg(functions.max(dsStocks.col("price").alias("Max")),
						functions.min(dsStocks.col("price").alias("Min")),
						functions.sum(dsStocks.col("volume")).alias("Volume"));

		

		Dataset<KafkaOutputBO> dsKafka = aggResult.map(
				new KafkaOutputMapper(),
				Encoders.bean(KafkaOutputBO.class));
		
		dsKafka.writeStream()
        .format("io.iguaz.v3io.spark.sql.kv").outputMode("complete")
    .option ("Key","key")
    .option ("container-id",1).option("path","http://10.90.1.71:8081/1025/".format("/taxi_example/", "driver_kv/")).start();
        
        
		
	}

	public void shutdown() {
		sparkSession.close();
	}

}
