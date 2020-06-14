package com.jortage.proxy;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.google.gson.JsonObject;

import com.google.common.base.Charsets;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;

import kotlin.Pair;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.brotli.BrotliInterceptor;

public class RivetTest {

	public static void main(String[] args) throws Exception {
		OkHttpClient client = new OkHttpClient.Builder()
				.addInterceptor(BrotliInterceptor.INSTANCE)
				.build();
		JsonObject obj = new JsonObject();
		obj.addProperty("sourceUrl", "http://example.com/nothing.png");
		obj.addProperty("destinationPath", "test.png");
		String payload = obj.toString();
		String accessKey = "test";
		String secretKey = "test";
		String date = DateTimeFormatter.ISO_INSTANT.format(Instant.now());
		Mac mac = assertSuccess(() -> Mac.getInstance("HmacSHA512"));
		byte[] payloadBytes = payload.getBytes(Charsets.UTF_8);
		
		assertSuccess(() -> mac.init(new SecretKeySpec(secretKey.getBytes(Charsets.UTF_8), "RAW")));
		mac.update((accessKey+":"+date+":"+payload).getBytes(Charsets.UTF_8));
		byte[] macBys = mac.doFinal();
		String auth = accessKey+":"+BaseEncoding.base64().encode(macBys)+":"+date;
		try (Response res = client.newCall(new Request.Builder()
				.url("http://localhost:23280/retrieve")
				.post(RequestBody.create(payloadBytes, MediaType.parse("application/json; charset=utf-8")))
				.header("Rivet-Auth", auth)
				.header("User-Agent", "Jortage Rivet Test")
				.build()).execute()) {
			Request req = res.networkResponse().request();
			System.out.println(req.method()+" "+req.url());
			for (Pair<? extends String, ? extends String> pair : req.headers()) {
				System.out.println(pair.getFirst()+": "+pair.getSecond());
			}
			System.out.println();
			System.out.println(payload);
			System.out.println();
			System.out.println();
			System.out.println(res.code()+" "+res.message());
			for (Pair<? extends String, ? extends String> pair : res.headers()) {
				System.out.println(pair.getFirst()+": "+pair.getSecond());
			}
			System.out.println();
			ByteStreams.copy(res.body().byteStream(), System.out);
		}
	}
	
	private interface ExceptableRunnable { void run() throws Exception; }
	private interface ExceptableSupplier<T> { T get() throws Exception; }
	
	private static void assertSuccess(ExceptableRunnable r) {
		try {
			r.run();
		} catch (Exception e) {
			throw new AssertionError(e);
		}
	}
	private static <T> T assertSuccess(ExceptableSupplier<T> s) {
		try {
			return s.get();
		} catch (Exception e) {
			throw new AssertionError(e);
		}
	}
	
}
