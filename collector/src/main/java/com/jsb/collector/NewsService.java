package com.jsb.collector;

import java.net.URLEncoder;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.stereotype.Component;

@Component
public class NewsService {
	
	//TODO: make an interface
	private static final String API_URL = "http://newsapi.org/v2/";
	private static final String API_KEY = "&apiKey=f282b6f9013d45289bb5964415d15418";
	
	/**
	 * Request "everything" method in NewsApi, we have different options
	 * @param query, from, sort
	 * @return JSONObject
	 */
	public JSONObject requestEverything(String query) {
		String url = "";
		try {
			String query_encoded = URLEncoder.encode(query, "UTF-8");
			url = API_URL + "everything?q=" + query_encoded + API_KEY;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return get(url);
	}
	
	public JSONObject requestEverything(String query, String from) {
		String url = "";
		try {
			String query_encoded = URLEncoder.encode(query, "UTF-8");
			url = API_URL + "everything?q=" + query_encoded + "&from=" + from + API_KEY;
			System.out.println(url);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return get(url);
	}
	
	public JSONObject requestEverything(String query, String from, String sort) {
		String url = "";
		try {
			String query_encoded = URLEncoder.encode(query, "UTF-8");
			url = API_URL + "everything?q=" + query_encoded + "&from=" + from + "&sortBy=" + sort + API_KEY;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return get(url);
	}
	
	/**
	 * Request top headlines from NewsAPI
	 * @param country
	 * @return JSONObject
	 */
	public JSONObject requestHeadlines(String country) {
		String url = API_URL + "top-headlines?country=" + country + API_KEY;
		return get(url);
	}
	
	/**
	 * Create request from Apache library
	 * @param url
	 * @return JSONOBject
	 */
	private JSONObject get(String url) {
		JSONObject result = new JSONObject();
		try {
			// Creating a HttpClient object
			CloseableHttpClient httpclient = HttpClients.createDefault();
			
			// Creating a HttpGet object
			HttpGet httpget = new HttpGet(url);

			// Executing the Get request
			HttpResponse httpresponse = httpclient.execute(httpget);

			HttpEntity entity = httpresponse.getEntity();

			if (entity != null) {
				// return it as a String
				String response = EntityUtils.toString(entity);
				JSONObject json = new JSONObject(response);
				String status = json.get("status").toString();
				
				if (status.contentEquals("ok")) {
					result = json;
				}
			}
			httpclient.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
}
