package com.mongodb.atlas;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.mongodb.okhttp.AuthenticationCacheInterceptor;
import com.mongodb.okhttp.CachingAuthenticatorDecorator;
import com.mongodb.okhttp.digest.CachingAuthenticator;
import com.mongodb.okhttp.digest.Credentials;
import com.mongodb.okhttp.digest.DigestAuthenticator;

import okhttp3.OkHttpClient;
import okhttp3.logging.HttpLoggingInterceptor;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

public class AtlasServiceGenerator {

    private static final String BASE_URL = "https://cloud.mongodb.com/api/atlas/v1.0/";

    private static Retrofit.Builder builder = new Retrofit.Builder().baseUrl(BASE_URL)
            .addConverterFactory(GsonConverterFactory.create());

    private static Retrofit retrofit = builder.build();
    
    private static OkHttpClient client;

    //private static OkHttpClient.Builder httpClient = new OkHttpClient.Builder();

    private static HttpLoggingInterceptor logging = new HttpLoggingInterceptor()
            .setLevel(HttpLoggingInterceptor.Level.BASIC);

//    public static <S> S createService(Class<S> serviceClass) {
//        if (!httpClient.interceptors().contains(logging)) {
//            httpClient.addInterceptor(logging);
//            builder.client(httpClient.build());
//            retrofit = builder.build();
//        }
//        return retrofit.create(serviceClass);
//    }

    public static <S> S createService(Class<S> serviceClass, String username, String apiKey) {
        final DigestAuthenticator authenticator = new DigestAuthenticator(new Credentials(username, apiKey));
        final Map<String, CachingAuthenticator> authCache = new ConcurrentHashMap<>();
        
        client = new OkHttpClient.Builder()
                .authenticator(new CachingAuthenticatorDecorator(authenticator, authCache))
                .addInterceptor(new AuthenticationCacheInterceptor(authCache))
               .addInterceptor(logging)
                .build();
        
            builder.client(client);
            retrofit = builder.build();

        return retrofit.create(serviceClass);
    }
    
    public static void shutdown() {
    	client.connectionPool().evictAll();
    }

}
