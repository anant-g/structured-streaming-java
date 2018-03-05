package com.processor.driver;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.processor.driver.StockDataProcessingDriver;
import com.processor.util.Util;

public class StockDataProcessorDriverTest {

	private static Producer<String, String> producer;
	private static Consumer<String, String> consumer;
	private static Properties kafkaProperties = new Properties();
	private static Properties props = new Properties();
	private static AdminClient client;

	@BeforeClass
	public static void initializeProperties() {
		kafkaProperties.put("metadata.broker.list", "localhost:9092");
		kafkaProperties.put("bootstrap.servers", "localhost:9092");
		kafkaProperties.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		kafkaProperties.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		kafkaProperties.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		kafkaProperties.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		kafkaProperties.put("enable.auto.commit", "true");
		kafkaProperties.put("auto.commit.interval.ms", "1000");
		kafkaProperties.put("group.id", "group");
		kafkaProperties.put("auto.offset.reset", "earliest");

		client = AdminClient.create(kafkaProperties);

	}

	@Before
	public void initialize() {
		props = Util.loadProperties("testconfig.properties",
				this.getClass());

		NewTopic inputTopic = new NewTopic(props.getProperty("inputTopicName"),
				1, Short.parseShort("1"));
		NewTopic outputTopic = new NewTopic(
				props.getProperty("outputTopicName"), 1, Short.parseShort("1"));
		Collection<NewTopic> topics = new ArrayList<NewTopic>();
		topics.add(inputTopic);
		topics.add(outputTopic);
		client.createTopics(topics);

		producer = new KafkaProducer<String, String>(kafkaProperties);

		String topic = props.getProperty("inputTopicName");
		ProducerRecord<String, String> record1 = new ProducerRecord<String, String>(
				topic, "1", "100,2017-08-16 15:35:45,AAA,BUYER,SELLER,10,100");
		ProducerRecord<String, String> record2 = new ProducerRecord<String, String>(
				topic, "2", "102,2017-08-16 15:35:45,BBB,BUYER,SELLER,8,10");
		ProducerRecord<String, String> record3 = new ProducerRecord<String, String>(
				topic, "3", "103,2017-08-16 15:45:45,AAA,BUYER,SELLER,15,80");
		ProducerRecord<String, String> record4 = new ProducerRecord<String, String>(
				topic, "4", "104,2017-08-16 15:50:45,BBB,BUYER,SELLER,2,400");
		producer.send(record1);
		producer.send(record2);
		producer.send(record3);
		producer.send(record4);

		producer.close();

		consumer = new KafkaConsumer<String, String>(kafkaProperties);
	}

	@Test
	public void testProcessor() {
		StockDataProcessingDriver driver = new StockDataProcessingDriver(
				"testconfig.properties", "shutdownHook");
		driver.run();
		driver.shutdown();
		verifyOutput();

	}

	public void verifyOutput() {
		consumer.subscribe(new ArrayList<String>(
				Arrays.asList(props.getProperty("outputTopicName"))));
		ConsumerRecords<String, String> records = consumer.poll(100);
		assertEquals("count of output rows", 2, records.count());

		Map<String, String> output = new HashMap<>();
		records.forEach(record -> {
			output.put(record.key(), record.value());
		});

		assertEquals("row1", "15|BBB|2.0|8.0|410",
				output.get("BBB 2017-01-16 15:00:00.0 2017-01-16 16:00:00.0"));
		assertEquals("row2", "15|AAA|10.0|15.0|180",
				output.get("AAA 2017-01-16 15:00:00.0 2017-01-16 16:00:00.0"));
		consumer.close();
	}

	@AfterClass
	public static void cleanup() {
		System.out.println("starting cleanup....");
		Collection<String> topics = new ArrayList<String>();
		topics.add(props.getProperty("outputTopicName"));
		topics.add(props.getProperty("inputTopicName"));
		client.deleteTopics(topics);

		try {
			Thread.sleep(10000);
			FileUtils.deleteDirectory(
					new File(props.getProperty("checkPointSink")));
			FileUtils.deleteDirectory(
					new File(props.getProperty("checkpointRaw")));
			FileUtils
					.deleteDirectory(new File(props.getProperty("outputPath")));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
