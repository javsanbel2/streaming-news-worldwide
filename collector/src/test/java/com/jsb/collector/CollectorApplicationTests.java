package com.jsb.collector;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import com.jsb.collector.bigquery.BigQueryService;

@SpringBootTest
class CollectorApplicationTests {

	@Autowired
	private BigQueryService bigqueryService;
	
	@Test
	void contextLoads() {
	}
	
	@Test
	void bigQueryServiceWorks() throws Exception {
		bigqueryService = new BigQueryService();
	}
	
	@Test
	void queriesWorks() throws Exception {
		bigqueryService = new BigQueryService();
		// 2.2 mb query
		String queryString = "SELECT repository.has_downloads FROM `bigquery-public-data.samples.github_nested` LIMIT 5;";
		Double querySize = bigqueryService.calculateQuerySize(queryString);
		
		// Expected value 2.179342269897461
		assertTrue(querySize != -1.0, "The query did not run");
		assertTrue(querySize < 3, "Method not caculate precisely");
	}
	

}
