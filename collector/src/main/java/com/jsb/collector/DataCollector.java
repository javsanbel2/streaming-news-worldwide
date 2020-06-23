package com.jsb.collector;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import org.codehaus.jettison.json.JSONObject;
import org.springframework.stereotype.Component;

@Component
public class DataCollector {

	public static void main(String[] args) {
		// Getting datetime from now
		String date = LocalDate.now().toString();
		String time = LocalTime.now().minusHours(10).format(DateTimeFormatter.ofPattern("HH:mm:ss"));
		String threshold_datetime = date + "T" + time;
		System.out.println(threshold_datetime);
		
		// Topic to search
		String topic = "empowerment AND woman";
		
		// Request information
		NewsService newsService = new NewsService();
		JSONObject res = newsService.requestEverything(topic, threshold_datetime);
		System.out.println(res);
	}
	
	/**
	 * Method to get streaming data from a certain list of keywords (topic). This method will
	 * call himself every 30 minutes to get more information. This news will be always sorted by date
	 * @param topic
	 */
	public void getStreamingDataFromCertainTopic(String topic) {
	}
	
}
