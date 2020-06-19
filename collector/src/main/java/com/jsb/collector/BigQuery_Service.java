package com.jsb.collector;

import java.io.FileInputStream;
import java.util.UUID;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

public class BigQuery_Service {

	private static String path_credentials = "/home/javi/Desktop/Workspaces/Java/collector/collector-properties.json";
	private BigQuery bigquery;

	public BigQuery_Service() throws Exception {
		// Instantiate service
		this.bigquery = BigQueryOptions.newBuilder().setProjectId("tae-collector")
				.setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(path_credentials))).build()
				.getService();
	}

	public TableResult query(String query) {
		TableResult result;
		// Create query
		QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).setUseLegacySql(false).build();

		// Create Job id
		JobId jobId = JobId.of(UUID.randomUUID().toString());
		Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

		try {
			// Wait for the query to complete.
			queryJob = queryJob.waitFor();

			// Get the results.
			result = queryJob.getQueryResults();
		} catch (Exception e) {
			// Check for errors
			if (queryJob == null) {
				throw new RuntimeException("Job no longer exists");
			} else if (queryJob.getStatus().getError() != null) {
				// You can also look at queryJob.getStatus().getExecutionErrors() for all
				// errors, not just the latest one.
				throw new RuntimeException(queryJob.getStatus().getError().toString());
			} else {
				throw new RuntimeException("Error");
			}

		}
		return result;

	}
}
