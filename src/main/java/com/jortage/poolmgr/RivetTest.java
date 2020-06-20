package com.jortage.poolmgr;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.common.base.Charsets;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;

import kotlin.Pair;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.brotli.BrotliInterceptor;
import okio.BufferedSink;
import okio.Okio;

public class RivetTest {

	private static final String HOST = "http://localhost:23280";
	
	public static void main(String[] args) throws Exception {
		OkHttpClient client = new OkHttpClient.Builder()
				.addInterceptor(BrotliInterceptor.INSTANCE)
				.addNetworkInterceptor((chain) -> {
					Request req = chain.request();
					System.out.println("\u001B[0m\u001B[7m "+req.method()+" \u001B[0m "+req.url());
					for (Pair<? extends String, ? extends String> pair : req.headers()) {
						System.out.println("\u001B[38;5;117m"+pair.getFirst()+": \u001B[38;5;213m"+pair.getSecond());
					}
					System.out.println("\u001B[0m");
					if (!req.body().isOneShot() && req.body().contentType().toString().startsWith("application/json")) {
						ByteArrayOutputStream baos = new ByteArrayOutputStream();
						BufferedSink bs = Okio.buffer(Okio.sink(baos));
						req.body().writeTo(bs);
						bs.emit();
						JsonObject obj = new Gson().fromJson(new String(baos.toByteArray(), Charsets.UTF_8), JsonObject.class);
						prettyPrint(obj, "");
					} else {
						long len = req.body().contentLength();
						System.out.println("<"+(len == -1 ? "?" : len)+" bytes>");
					}
					System.out.println();
					System.out.println();
					Response res = chain.proceed(req);
					if (res.isSuccessful()) {
						System.out.print("\u001B[102m\u001B[30m");
					} else if (res.isRedirect()) {
						System.out.print("\u001B[37m");
					} else if (res.code() >= 400 && res.code() <= 499) {
						System.out.print("\u001B[105m\u001B[30m");
					} else if (res.code() >= 500 && res.code() <= 599) {
						System.out.print("\u001B[101m\u001B[37m");
					} else {
						System.out.print("\u001B[46m\u001B[30m");
					}
					System.out.println(" "+res.code()+" \u001B[0m "+res.message());
					for (Pair<? extends String, ? extends String> pair : res.headers()) {
						System.out.println("\u001B[38;5;117m"+pair.getFirst()+": \u001B[38;5;213m"+pair.getSecond());
					}
					System.out.println("\u001B[0m");
					if (res.body().contentLength() != 0) {
						if (res.body().contentType() != null && res.body().contentType().toString().startsWith("application/json")) {
							JsonObject obj = new Gson().fromJson(res.body().byteString().utf8(), JsonObject.class);
							prettyPrint(obj, "");
						} else {
							ByteStreams.copy(res.body().byteStream(), System.out);
						}
						System.out.println();
						System.out.println();
					}
					return res;
				})
				.build();
		JsonObject obj = new JsonObject();
		obj.addProperty("sourceUrl", "https://blob.jortage.com/site/jortage_header_logo_dark.png");
		obj.addProperty("destinationPath", "jortage_logo.png");
//		doRivetRequest(client, "/retrieve", "test", "test", "application/json; charset=utf-8", ByteSource.wrap(obj.toString().getBytes(Charsets.UTF_8)), true);
		doRivetRequest(client, "/upload/fastorange.png?b28e0f25d21559880fdd027f35b2359f810bc88ae01ed1220ce85a2038ab584332402270840cb7bff3bd6e53dd7e8d2edf9078d05baf503e1f646dc74b39118a",
				"test", "test", "image/png", Files.asByteSource(new File("fastorange.png")), false);
	}
	
	private static void prettyPrint(JsonElement ele, String indent) {
		if (ele instanceof JsonObject) {
			System.out.println("{");
			String origIndent = indent;
			indent = indent+"  ";
			for (Map.Entry<String, JsonElement> en : ele.getAsJsonObject().entrySet()) {
				System.out.print(indent+"\u001B[38;5;117m"+en.getKey()+": \u001B[0m");
				prettyPrint(en.getValue(), indent);
			}
			System.out.println(origIndent+"}");
		} else if (ele instanceof JsonArray) {
			System.out.println("[");
			String origIndent = indent;
			indent = indent+"  ";
			for (JsonElement e : ele.getAsJsonArray()) {
				System.out.print(indent);
				prettyPrint(e, indent);
			}
			System.out.println(origIndent+"]");
		} else if (ele instanceof JsonNull) {
			System.out.println("\u001B[90mnull\u001B[0m");
		} else if (ele instanceof JsonPrimitive) {
			if (((JsonPrimitive) ele).isString()) {
				System.out.println("\u001B[38;5;213m"+ele+"\u001B[0m");
			} else {
				System.out.println("\u001B[38;5;48m"+ele+"\u001B[0m");
			}
		}
	}

	private static void doRivetRequest(OkHttpClient client, String target, String accessKey, String secretKey,
			String contentType, ByteSource payload, boolean signPayload) throws IOException {
		String date = DateTimeFormatter.ISO_INSTANT.format(Instant.now());
		Mac mac = assertSuccess(() -> Mac.getInstance("HmacSHA512"));
		byte[] payloadBytes = signPayload ? payload.read() : new byte[0];
		String payloadStr = new String(payloadBytes, Charsets.UTF_8);
		
		assertSuccess(() -> mac.init(new SecretKeySpec(secretKey.getBytes(Charsets.UTF_8), "RAW")));
		mac.update((target+":"+accessKey+":"+date+":"+payloadStr).getBytes(Charsets.UTF_8));
		byte[] macBys = mac.doFinal();
		String auth = accessKey+":"+BaseEncoding.base64().encode(macBys)+":"+date;
		try (Response res = client.newCall(new Request.Builder()
				.url(HOST+target)
				.post(signPayload ? RequestBody.create(payloadBytes, MediaType.parse(contentType))
						: new ByteSourceRequestBody(payload, MediaType.parse(contentType)))
				.header("Rivet-Auth", auth)
				.header("User-Agent", "Jortage Rivet Test")
				.header("Expect", signPayload ? "102-processing" : "100-continue")
				.build()).execute()) {
		}
	}
	
	public static class ByteSourceRequestBody extends RequestBody {

		private final ByteSource source;
		private final MediaType type;
		
		public ByteSourceRequestBody(ByteSource source, MediaType type) {
			this.source = source;
			this.type = type;
		}

		@Override
		public MediaType contentType() {
			return type;
		}
		
		@Override
		public long contentLength() throws IOException {
			return source.size();
		}

		@Override
		public void writeTo(BufferedSink sink) throws IOException {
			source.copyTo(sink.outputStream());
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
