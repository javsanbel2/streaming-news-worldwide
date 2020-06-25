package com.jsb.collector;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

import com.jsb.collector.kafka.KafkaProducerService;

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
	// ****   News Service tests  *****
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
		JSONObject data = newsService.requestEverything("bitcoin", "2020-05-24");
		String status = data.get("status").toString();
		Boolean itWorks = status.contentEquals("ok");
		assertTrue(itWorks);
	}
	
	// Request everything method with 3 params
	@Test
	void requestEverything3() throws JSONException {
		// Run method
		JSONObject data = newsService.requestEverything("bitcoin", "2020-05-24", "publishedAt");
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
	// ****  Kafka Producer Service  *****
	// ***********************************
	
	// Request headlines by country
	@Test
	void sendKafkaMessageWorks() {
		// If the method continue is because the method works
		Boolean res = false;
		producer.sendKafkaMessage(this.topicFromKafka, "test");
		res = true;
		assertTrue(res);
	}
	
	// ***********************************
	// ********  Data collector  *********
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
