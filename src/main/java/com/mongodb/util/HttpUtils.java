package com.mongodb.util;

import java.io.IOException;

import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

public class HttpUtils {
    
    private static Logger logger = LoggerFactory.getLogger(HttpUtils.class);

    private CloseableHttpClient httpClient;
    
    private Gson gson;

    public HttpUtils() {
        httpClient = HttpClients.custom().build();
        gson = new GsonBuilder().create();
    }

    public String doGetAsString(String url) {
        HttpGet httpGet = new HttpGet(url);
        try {
            CloseableHttpResponse response = httpClient.execute(httpGet);
            int responseCode = response.getStatusLine().getStatusCode();
            if (responseCode != HttpStatus.SC_OK) {
                throw new IllegalStateException(
                        String.format("Unexecpected http response status code %s", responseCode));
            }
            String responseStr = EntityUtils.toString(response.getEntity());
            return responseStr;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
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

}
