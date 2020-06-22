package com.jsb.collector;

import org.springframework.beans.factory.annotation.Autowired;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.jsb.collector.bigquery.BigQueryService;
import com.jsb.collector.kafka.Producer;

public class DataCollector {

	private final Producer producer;

	@Autowired
	DataCollector(Producer producer) {
		this.producer = producer;
	}

	// *********************
	// Getting GitHub Dataset
	// *********************
	public void getGitHubDataset() {
		try {
			String queryString = "SELECT repository.has_downloads FROM `bigquery-public-data.samples.github_nested` LIMIT 5;";
			BigQueryService bigquery = new BigQueryService();

			TableResult res = bigquery.query(queryString);
			
			for (FieldValueList row : res.iterateAll()) {
				System.out.println(row);
				this.producer.sendMessage(row.toString());
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getCause());
			throw new RuntimeException(e.getMessage());
		}
	}

	public void getHackerNewsDataset() {
		// .. some queries to get dataset
	}

//	      String url = row.get("url").getStringValue();
//	      long viewCount = row.get("view_count").getLongValue();
//	      System.out.printf("url: %s views: %d%n", url, viewCount);
}
