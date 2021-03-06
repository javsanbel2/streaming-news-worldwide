package com.jsb.collector;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.jsb.collector.kafka.KafkaProducerService;

@Component
public class DataCollector {
	
	@Value("${kafka.topic}")
    private String topicFromKafka;
	
	@Value("${api.query}")
    private String queryFromProperties;
	
	@Autowired
	private NewsService newsService;
	
	@Autowired
	private KafkaProducerService producerService;
	
	private static final Logger log = LoggerFactory.getLogger(DataCollector.class);

	/**
	 * Method to get streaming data from a certain list of keywords (topic). This method will
	 * call himself every 30 minutes to get more information. This news will be always sorted by date
	 * @param topic
	 */
	@Scheduled(fixedRate = 300000) // Every 5 min
	public void getStreamingDataFromCertainQuery() {
		log.info("Requesting data to NewsAPI" + this.queryFromProperties);
		JSONObject data = requestData(this.queryFromProperties);
		
		if (!this.checkIfEmptyNewArticles(data)) {
			producerService.sendKafkaMessage(this.topicFromKafka, data.toString());
		} else {
			log.info("Requested data response empty");
		}
		
	}
	
	/**
	 * Request information to NewsAPI from the date of right now
	 * @param topic
	 * @return
	 */
	public JSONObject requestData(String query) {
		// Getting datetime from now and parse
		String date = LocalDate.now().toString();
		LocalTime time = LocalTime.now().minusHours(1).minusMinutes(30);
		
		String from = date + "T" + time.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
		String to = date + "T" + time.plusMinutes(5).format(DateTimeFormatter.ofPattern("HH:mm:ss"));
		
		// Request information
		JSONObject res = newsService.requestEverything(query, from, to);

		return res;
	}
	
	// Checking if the request is empty
	public Boolean checkIfEmptyNewArticles(JSONObject data) {
		Boolean res = true;
		try {
			JSONArray articles = (JSONArray) data.get("articles");
			res = articles.isNull(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return res;
	}
	
}
