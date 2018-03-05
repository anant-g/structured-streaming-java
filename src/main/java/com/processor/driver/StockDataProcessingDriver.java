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

public class StockDataProcessingDriver { 

	private SparkSession sparkSession;
	private String outputMode;
	private Map<String, String> config = new HashMap<String, String>();
	private Properties prop;
	private String propFileName;
	private String shutdownHook;

	public StockDataProcessingDriver() {
		this.propFileName = "config.properties";
		init();
	}

	// constructor for integration tests
	public StockDataProcessingDriver(String propertyFile, String shutdownHook) {
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
		StockDataProcessingDriver driver = new StockDataProcessingDriver();
		driver.run();

	}

	public void run() {
		Dataset<Row> df = this.sparkSession.readStream().format("kafka")
				.options(this.config).load();
		Dataset<Row> df_value = df.selectExpr("CAST(value AS STRING)");
		Dataset<StockBO> dsStocks = df_value.map(new StockMapper(),
				Encoders.bean(StockBO.class));

		// raw data storage

		StreamingQuery rawStorage = dsStocks.writeStream().format("parquet")
				.option("startingOffsets", "earliest")
				.option("checkpointLocation",
						this.prop.getProperty("checkpointRaw"))
				.option("path", this.prop.getProperty("outputPath"))
				.partitionBy("stockCode").start();

		Dataset<Row> aggResult = dsStocks
				.groupBy(dsStocks.col("stockCode").alias("Stock"),
						functions.window(dsStocks.col("timestamp"), "1 hour")
								.alias("Hour"))
				.agg(functions.max(dsStocks.col("price").alias("Max")),
						functions.min(dsStocks.col("price").alias("Min")),
						functions.sum(dsStocks.col("volume")).alias("Volume"));

		StreamingQuery queryAgg = null;

		// kafka change log in append mode

		if (outputMode.equalsIgnoreCase("kafka")) {
			Dataset<KafkaOutputBO> dsKafka = aggResult.map(
					new KafkaOutputMapper(),
					Encoders.bean(KafkaOutputBO.class));
			queryAgg = dsKafka
					.selectExpr("CAST(key AS STRING) AS key",
							"CAST(value AS STRING) as value")
					.writeStream().format("kafka").outputMode("complete")
					.option("kafka.bootstrap.servers",
							this.prop.getProperty(
									"kafka.bootstrap.servers.sink"))
					.option("topic", this.prop.getProperty("outputTopicName"))
					.option("checkpointLocation",
							this.prop.getProperty("checkPointSink"))
					.start();
		}
		// writing to mysql with foreach sink in update mode

		if (outputMode.equalsIgnoreCase("jdbc")) {
			JdbcSink sink = new JdbcSink();
			queryAgg = aggResult.writeStream().outputMode("update")
					.foreach(sink).start();
		}

		try {
			if (this.shutdownHook != null) {
				rawStorage.processAllAvailable();
				queryAgg.processAllAvailable();
				queryAgg.stop();
				rawStorage.stop();
			} else {
				rawStorage.awaitTermination();
				queryAgg.awaitTermination();
			}

		} catch (StreamingQueryException e) {
			shutdown();
			e.printStackTrace();
		}
	}

	public void shutdown() {
		sparkSession.close();
	}

}
