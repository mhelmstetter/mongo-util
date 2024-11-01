package com.mongodb.util;

import java.io.IOException;

import org.apache.http.HttpStatus;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class HttpUtils {

	private static Logger logger = LoggerFactory.getLogger(HttpUtils.class);

	private CloseableHttpClient httpClient;

	private Gson gson;

	public HttpUtils() {
		httpClient = HttpClients.custom().build();
		gson = new GsonBuilder().create();
	}
	
	public String doGetAsString(String url, int maxTries) throws IOException {
		
		HttpGet httpGet = new HttpGet(url);

		CloseableHttpResponse response = null;
		int sleep = 1000;
		for (int attempt = 1; attempt <= maxTries; attempt++) {
			try {
				response = httpClient.execute(httpGet);
				break;
			} catch (IOException ioe) {
				if (maxTries > 1) {
					logger.warn("connect error, attempt {}, will retry: {}", attempt, ioe.getMessage());
				}
			}
			try {
				Thread.sleep(sleep * attempt);
			} catch (InterruptedException e) {
			}
		}
		
		if (response == null) {
			throw new IOException(String.format("Unable to connect and/or get response, url: %s", url));
		}

		int responseCode = response.getStatusLine().getStatusCode();
		if (responseCode != HttpStatus.SC_OK) {
			throw new IllegalStateException(String.format("Unexecpected http response status code %s", responseCode));
		}
		String responseStr = EntityUtils.toString(response.getEntity());
		return responseStr;
	}

	public String doGetAsString(String url) throws IOException {
		return doGetAsString(url, 5);

	}

	public JsonObject doGetAsJsonObject(String url) throws ClientProtocolException, IOException {
		String json = doGetAsString(url);
		try {
			JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();
			return jsonObject;
		} catch (JsonSyntaxException e) {
			logger.error("Invalid JSON format: " + e.getMessage());
		}
		return null;
	}

	public <T> T doGetAsObject(String url, Class<T> classOfT) throws IOException {
		String json = doGetAsString(url);
		try {
			return gson.fromJson(json, classOfT);
		} catch (JsonSyntaxException jse) {
			logger.error("Error unmarshalling json", jse);
			return null;
		}

	}

	public String doPostAsString(String url, String data) throws IOException {
		HttpPost httpPost = new HttpPost(url);
		httpPost.setEntity(new StringEntity(data, "UTF-8"));
		httpPost.setHeader("Content-Type", "application/json");

		CloseableHttpResponse response = httpClient.execute(httpPost);
		int responseCode = response.getStatusLine().getStatusCode();
		if (responseCode != HttpStatus.SC_OK) {
			throw new IllegalStateException(String.format("Unexpected http response status code %s", responseCode));
		}
		String responseStr = EntityUtils.toString(response.getEntity());
		return responseStr;
	}

}
