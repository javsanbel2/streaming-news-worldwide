package com.jsb.collector.bigquery;

public class DataCollector {
	public static void main(String[] args) throws Exception {
		String queryString = "SELECT repository.has_downloads FROM `bigquery-public-data.samples.github_nested` LIMIT 5;";
		BigQuery_Service bigquery = new BigQuery_Service();
		
//		TableResult res = bigquery.query(queryString);
		System.out.println(bigquery.calculateQuerySize(queryString));
	    // Print all pages of the results.
//	    for (FieldValueList row : res.iterateAll()) {
//	    	System.out.println(row);
//	      String url = row.get("url").getStringValue();
//	      long viewCount = row.get("view_count").getLongValue();
//	      System.out.printf("url: %s views: %d%n", url, viewCount);
//	    }

	}

}
