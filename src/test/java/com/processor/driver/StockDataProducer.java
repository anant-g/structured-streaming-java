package com.processor.driver;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class StockDataProducer {

	private KafkaProducer<String, String> kafkaProducer;

	public static void main(String[] args) {
		StockDataProducer driver = new StockDataProducer();
		driver.run();

	}

	public void run() {
		Properties props = new Properties();

		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");

		this.kafkaProducer = new KafkaProducer<String, String>(props);
		int i = 0;
		/**
		 * producing random batches of data every second.
		 */
		Random random = new Random();

		while (true) {
			for (i = 0; i <= 10; i++) {
				kafkaProducer.send(new ProducerRecord<String, String>(
						"market-data-input", "" + i,
						"1" + i + ",2017-08-16 15:35:45,ANZ"
								+ random.nextInt(10) + ",buyer,seller,"
								+ 100.0 * random.nextInt(100) + ","
								+ 10 * random.nextInt(100)));
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
				kafkaProducer.close();
			}
			System.out.println("write success");
		}

	}

}
