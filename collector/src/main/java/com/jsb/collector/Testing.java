package com.jsb.collector;

import com.jsb.collector.kafka.Producer;

public class Testing {
	public static void main(String[] args) {
		Producer producer = new Producer();
		DataCollector dataCollector = new DataCollector(producer);
		dataCollector.getGitHubDataset();
		
	}
}
