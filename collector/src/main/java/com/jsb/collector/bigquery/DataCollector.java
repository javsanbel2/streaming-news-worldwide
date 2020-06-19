package com.jsb.collector.bigquery;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;

public class DataCollector {
	
	public void getGitHubDataset() {
		// .. some queries to get dataset
	}
	
	public void getHackerNewsDataset() {
		// .. some queries to get dataset
	}
	
	public static void main(String[] args) throws Exception {
		String queryString = "SELECT repository.has_downloads FROM `bigquery-public-data.samples.github_nested` LIMIT 5;";
		BigQueryService bigquery = new BigQueryService();
		
		TableResult res = bigquery.query(queryString);
//		System.out.println(bigquery.calculateQuerySize(queryString));
	    // Print all pages of the results.
	    for (FieldValueList row : res.iterateAll()) {
	    	System.out.println(row);
//	      String url = row.get("url").getStringValue();
//	      long viewCount = row.get("view_count").getLongValue();
//	      System.out.printf("url: %s views: %d%n", url, viewCount);
	    }

	}

}
