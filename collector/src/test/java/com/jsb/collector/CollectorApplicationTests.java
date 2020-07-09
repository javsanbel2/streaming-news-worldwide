package com.jsb.collector;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.awt.Event;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

import com.jsb.collector.kafka.KafkaProducerService;
import org.awaitility.Awaitility;

@SpringBootTest
class CollectorApplicationTests {

	@Value("${kafka.topic}")
	private String topicFromKafka;

	@Autowired
	private NewsService newsService;

	@Autowired
	private KafkaProducerService producer;

	@Autowired
	private DataCollector collector;

	// Check if the class can autowire everything
	@Test
	void contextLoads() {
	}

	// ********************************
	// **** News Service tests *****
	// ********************************

	// Request everything method with 1 param
	@Test
	void requestEverything1() throws JSONException {
		// Run method
		JSONObject data = newsService.requestEverything("bitcoin");
		String status = data.get("status").toString();
		Boolean itWorks = status.contentEquals("ok");
		assertTrue(itWorks);
	}

	// Request everything method with 2 params
	@Test
	void requestEverything2() throws JSONException {
		// Run method
		String dateMinus21days = LocalDate.now().minusDays(21).toString();
		String dateMinus20days = LocalDate.now().minusDays(20).toString();
		JSONObject data = newsService.requestEverything("bitcoin", dateMinus21days, dateMinus20days);
		String status = data.get("status").toString();
		Boolean itWorks = status.contentEquals("ok");
		assertTrue(itWorks);
	}

	// Request everything method with 3 params
	@Test
	void requestEverything3() throws JSONException {
		// Run method
		String dateMinus21days = LocalDate.now().minusDays(21).toString();
		String dateMinus20days = LocalDate.now().minusDays(20).toString();
		JSONObject data = newsService.requestEverything("bitcoin", dateMinus21days, dateMinus20days, "publishedAt");
		String status = data.get("status").toString();
		Boolean itWorks = status.contentEquals("ok");
		assertTrue(itWorks);
	}

	// Request headlines by country
	@Test
	void requestHeadlines() throws JSONException {
		// Run method
		JSONObject data = newsService.requestHeadlines("us");
		String status = data.get("status").toString();
		Boolean itWorks = status.contentEquals("ok");
		assertTrue(itWorks);
	}

	// ***********************************
	// **** Kafka Producer Service *****
	// ***********************************

	@Test
	void checkKafkaWorks() {
		StreamsBuilder builder = new StreamsBuilder();
		builder.stream("input-topic").to("output-topic");
		Topology topology = builder.build();

		// setup test driver
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "DataCollectorTest");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		TopologyTestDriver testDriver = new TopologyTestDriver(topology, props);

		TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("input-topic", new StringSerializer(),
				new StringSerializer());
		inputTopic.pipeInput("Hello");

		TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic("output-topic",
				new StringDeserializer(), new StringDeserializer());

		assertTrue(outputTopic.readValue().contentEquals("Hello"));
		testDriver.close();
	}

	// Request headlines by country
	@Test
	void sendKafkaMessageWorks() {
		// If the method continue is because the method works
		Boolean res = false;
		producer.sendKafkaMessage(this.topicFromKafka, "test");
		res = true;
		assertTrue(res);
	}

	@Test
	void checkTimeoutKafka() throws InterruptedException, ExecutionException, TimeoutException {
		Properties consumerProperties = new Properties();
		consumerProperties.put("bootstrap.servers", "localhost:9092");
		consumerProperties.put("group.id", "localhost:2181");
		consumerProperties.put("zookeeper.session.timeout.ms", "6000");
		consumerProperties.put("zookeeper.sync.time.ms", "2000");
		consumerProperties.put("auto.commit.enable", "false");
		consumerProperties.put("auto.commit.interval.ms", "1000");
		consumerProperties.put("consumer.timeout.ms", "-1");
		consumerProperties.put("max.poll.records", "1");
		consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
		consumer.subscribe(Collections.singletonList("test"));

		final ArrayList<String> events = new ArrayList<>();

		org.awaitility.Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
			final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
			this.producer.sendKafkaMessageTest("test", "hello");
			for (final ConsumerRecord<String, String> record : records)
				events.add(record.value());

			assertTrue(1 < events.size());
			consumer.close();
		});
	}

	// ***********************************
	// ******** Data collector *********
	// ***********************************

	// Check if request data method works
	@Test
	void checkRequestCollector() throws JSONException {
		// If the method continue is because the method works
		JSONObject data = this.collector.requestData("bitcoin");
		String status = data.get("status").toString();
		Boolean itWorks = status.contentEquals("ok");
		assertTrue(itWorks);
	}

	// Check if empty method works
	@Test
	void checkEmptyMethod() {
		// If the method continue is because the method works
		JSONObject data = this.collector.requestData("dfasdfsadfasf");
		Boolean res = this.collector.checkIfEmptyNewArticles(data);
		assertTrue(res);
	}

	// Check if main method works
	@Test
	void checkMainMethod() {
		// If the method continue is because the method works, and do not throw an error
		Boolean res = false;
		this.collector.getStreamingDataFromCertainQuery();
		res = true;
		assertTrue(res);
	}
}
