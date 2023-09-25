package com.jortage.poolmgr.rivet;

import static com.google.common.base.Verify.verify;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import kotlin.Pair;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.options.PutOptions;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.jortage.poolmgr.FileReprocessor;
import com.jortage.poolmgr.Poolmgr;
import com.jortage.poolmgr.Queries;
import com.jortage.poolmgr.util.ByteSinkSource;
import com.jortage.poolmgr.util.FileByteSinkSource;
import com.jortage.poolmgr.util.MemoryByteSinkSource;

import com.google.common.base.CharMatcher;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.hash.HashingOutputStream;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.UncheckedExecutionException;

import okhttp3.HttpUrl;
import okhttp3.Interceptor;
import okhttp3.Interceptor.Chain;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.brotli.BrotliInterceptor;

public final class RivetHandler extends AbstractHandler {
	private static final Splitter RIVET_AUTH_SPLITTER = Splitter.on(':').limit(3);
	private static final CharMatcher HEX_MATCHER = CharMatcher.anyOf("0123456789abcdef");
	
	private static final String UA = "Jortage Rivet (+https://jortage.com/rivet.html)";
	
	private enum Temperature {
		FREEZING, COLD, WARM, HOT, SCALDING;
	}
	
	private enum RivetResult {
		/**
		 * The file was downloaded and added to the pool. Worst case.
		 */
		ADDED,
		/**
		 * The file was downloaded, and after hashing was found to be present in the pool already;
		 * the data was thrown away.
		 */
		PRESENT,
		/**
		 * The file was requested, and a blob redirect was found, so it short-circuited and avoided
		 * a download.
		 */
		FOUND,
		/**
		 * Someone else requested the exact same url within the past 10 minutes, so no requests
		 * were made at all. Best case.
		 */
		CACHED,
	}
	
	private final Gson gson;
	// synchronize on a mutex when loading URLs to avoid download races that would waste bandwidth
	private final Object retrieveMutex = new Object();
	private final Map<String, Pair<RivetResult, Temperature>> results = Maps.newHashMap();
	private final LoadingCache<String, HashCode> urlCache = CacheBuilder.newBuilder()
			.concurrencyLevel(1)
			.expireAfterWrite(10, TimeUnit.MINUTES)
			.<String, HashCode>removalListener((n) -> {
				synchronized (retrieveMutex) {
					results.remove(n.getKey());
				}
			})
			.build(new CacheLoader<String, HashCode>() {
				@Override
				public HashCode load(String url) throws Exception {
					ByteSinkSource bss = null;
					HttpUrl parsedUrl = HttpUrl.Companion.parse(url);
					checkIllegalUrl(null, parsedUrl);
					HashCode shortCircuit = checkShortCircuit(url, parsedUrl, Temperature.HOT);
					if (shortCircuit != null) return shortCircuit;
					try (Response headRes = client.newCall(new Request.Builder()
							.addHeader("User-Agent", UA)
							.url(parsedUrl)
							.head()
							.build()).execute()) {
						if (headRes.isSuccessful()) {
							shortCircuit = checkShortCircuit(url, headRes.request().url(), Temperature.WARM);
							if (shortCircuit != null) return shortCircuit;
							shortCircuit = checkShortCircuit(url, headRes.networkResponse().request().url(), Temperature.WARM);
							if (shortCircuit != null) return shortCircuit;
							try (Response getRes = client.newCall(new Request.Builder()
									.addHeader("User-Agent", UA)
									.url(headRes.request().url())
									.get()
									.build()).execute()) {
								if (getRes.isSuccessful()) {
									long len = getRes.body().contentLength();
									if (len == -1 || len > 8192) {
										bss = new FileByteSinkSource(File.createTempFile("jortage-proxy-", ".dat"), true);
									} else {
										bss = new MemoryByteSinkSource();
									}
									OutputStream sinkOut = bss.getSink().openStream();
									HashingOutputStream hos = new HashingOutputStream(Hashing.sha512(), sinkOut);
									try (InputStream in = getRes.body().byteStream()) {
										FileReprocessor.reprocess(in, hos);
									}
									hos.close();
									HashCode hash = hos.hash();
									String hashStr = hash.toString();
									String path = Poolmgr.hashToPath(hashStr);
									if (Queries.isMapped(Poolmgr.dataSource, hash)) {
										results.put(url, new Pair<>(RivetResult.PRESENT, Temperature.COLD));
									} else {
										Blob blob = Poolmgr.backingBlobStore.blobBuilder(path)
												.payload(bss.getSource())
												.contentLength(bss.getSource().size())
												.contentType(getRes.body().contentType().toString())
												.build();
										long size = bss.getSource().size();
										Poolmgr.backingBlobStore.putBlob(Poolmgr.bucket, blob,
												new PutOptions().setBlobAccess(BlobAccess.PUBLIC_READ).multipart(size > 8192));
										Queries.putPendingBackup(Poolmgr.dataSource, hash);
										Queries.putFilesize(Poolmgr.dataSource, hash, size);
										results.put(url, new Pair<>(RivetResult.ADDED, Temperature.FREEZING));
									}
									return hash;
								} else {
									throw new IOException("Unsuccessful response code to GET: "+getRes.code());
								}
							}
						} else {
							throw new IOException("Unsuccessful response code to HEAD: "+headRes.code());
						}
					} finally {
						if (bss != null) bss.close();
					}
				}

				private HashCode checkShortCircuit(String originalUrl, HttpUrl url, Temperature temp) {
					String publicHost = Poolmgr.publicHost.replaceFirst("^https?://", "");
					String fullHost = url.host();
					if (url.port() != (url.scheme().equals("https") ? 443 : 80)) {
						fullHost = fullHost+":"+url.port();
					}
					if (fullHost.equals(publicHost)) {
						List<String> segments = url.pathSegments();
						if (segments.size() == 4 && segments.get(0).equals("blobs")) {
							String prelude = segments.get(1)+segments.get(2);
							String hashStr = segments.get(3);
							if (hashStr.startsWith(prelude) && HEX_MATCHER.matchesAllOf(hashStr)) {
								HashCode hash = HashCode.fromString(hashStr);
								if (Queries.isMapped(Poolmgr.dataSource, hash)) {
									results.put(originalUrl, new Pair<>(RivetResult.FOUND, temp));
									return hash;
								}
							}
						}
					}
					return null;
				}
			});
	
	private OkHttpClient client;
	
	public RivetHandler() {
		this.gson = new Gson();
		Interceptor urlChecker = (chain) -> {
			Request req = chain.request();
			checkIllegalUrl(chain, req.url());
			Response resp = chain.proceed(req);
			if (resp.isRedirect() && resp.header("Location") != null) {
				String location = resp.header("Location");
				HttpUrl url = HttpUrl.Companion.parse(location);
				checkIllegalUrl(chain, url);
			}
			return resp;
		};
		this.client = new OkHttpClient.Builder()
				.addInterceptor(BrotliInterceptor.INSTANCE)
				.addInterceptor(urlChecker)
				.addNetworkInterceptor(urlChecker)
				.connectTimeout(8, TimeUnit.SECONDS)
				.build();
	}


	private void checkIllegalUrl(Chain chain, HttpUrl url) throws UnknownHostException, IOException {
		if (url.port() <= 0 || url.port() > 65535 || illegalPorts.contains(url.port())) {
			if (chain != null) chain.call().cancel();
			throw new IOException("Illegal host: Illegal port "+url.port());
		}
		String host = url.host();
		for (InetAddress inet : client.dns().lookup(host)) {
			if (inet.isAnyLocalAddress() || inet.isLinkLocalAddress() || inet.isLoopbackAddress()
					|| inet.isMulticastAddress() || inet.isSiteLocalAddress()) {
				if (chain != null) chain.call().cancel();
				throw new IOException("Illegal host: Illegal address "+inet.getHostAddress()+" ("+host+")");
			}
		}
	}


	private class RivetRequest {
		public final String identity;
		public final JsonObject json;
		private RivetRequest(String identity, JsonObject json) {
			this.identity = identity;
			this.json = json;
		}
	}
	
	@Override
	public void handle(String target, org.eclipse.jetty.server.Request baseRequest, HttpServletRequest req, HttpServletResponse res) throws IOException, ServletException {
		baseRequest.setHandled(true);
		if ("/retrieve".equals(target)) {
			Poolmgr.reloadConfigIfChanged();
			if (Poolmgr.readOnly) {
				jsonError(res, 503, "Currently in read-only maintenance mode; try again later");
				return;
			}
			RivetRequest rreq = authenticateAndParse(target, "POST", "application/json; charset=utf-8", true, req, res);
			if (rreq == null) return;
			if (!rreq.json.has("sourceUrl")) {
				jsonError(res, 400, "Must specify sourceUrl");
				return;
			}
			if (!rreq.json.has("destinationPath")) {
				jsonError(res, 400, "Must specify destinationPath");
				return;
			}
			String sourceUrl = rreq.json.get("sourceUrl").getAsString();
			if (!sourceUrl.startsWith("https://") && !sourceUrl.startsWith("http://")) {
				jsonError(res, 400, "sourceUrl must be http or https");
				return;
			}
			String destinationPath = rreq.json.get("destinationPath").getAsString();
			RivetResult retRes = null;
			Temperature temp = null;
			HashCode hash;
			res.sendError(102);
			synchronized (retrieveMutex) {
				try {
					if (urlCache.getIfPresent(sourceUrl) != null) {
						retRes = RivetResult.CACHED;
						temp = Temperature.SCALDING;
					}
					hash = urlCache.get(sourceUrl);
					if (retRes == null || temp == null) {
						Pair<RivetResult, Temperature> pair = results.get(sourceUrl);
						retRes = pair.getFirst();
						temp = pair.getSecond();
					}
				} catch (ExecutionException | UncheckedExecutionException e) {
					if (e.getMessage() != null) {
						if (e.getMessage().contains("Illegal host")) {
							jsonError(res, 400, "Illegal host");
							return;
						}
						if (e.getMessage().contains("Unsuccessful response")) {
							jsonError(res, 502, "Upstream error "+(e.getMessage().substring(e.getMessage().lastIndexOf(':')+1).trim()));
							return;
						}
						if (e.getMessage().contains("Failed to connect")) {
							jsonError(res, 502, "Upstream refused connection");
							return;
						}
						if (e.getMessage().contains("connect timed out")) {
							jsonError(res, 504, "Upstream timeout");
							return;
						}
					}
					jsonExceptionError(res, e, "sourceUrl: "+sourceUrl, "identity: "+rreq.identity);
					return;
				}
			}
			try {
				Queries.putMap(Poolmgr.dataSource, rreq.identity, destinationPath, hash);
				res.setStatus(200);
				JsonObject obj = new JsonObject();
				JsonObject result = new JsonObject();
				result.addProperty("name", retRes.name());
				result.addProperty("temperature", temp.name());
				obj.add("result", result);
				obj.addProperty("hash", hash.toString());
				sendJson(res, obj);
			} catch (Exception e) {
				jsonExceptionError(res, e, "sourceUrl: "+sourceUrl, "identity: "+rreq.identity, "hash: "+hash);
				return;
			}
		} else if (target.startsWith("/upload/")) {
			Poolmgr.reloadConfigIfChanged();
			if (Poolmgr.readOnly) {
				jsonError(res, 503, "Currently in read-only maintenance mode; try again later");
				return;
			}
			String expect = req.getHeader("Expect");
			if (expect == null || !expect.equals("100-continue")) {
				jsonError(res, 400, "Must expect continue");
				return;
			}
			String hashStr = req.getQueryString();
			if (hashStr == null || hashStr.length() != 128 || !HEX_MATCHER.matchesAllOf(hashStr)) {
				jsonError(res, 400, "Bad hash");
				return;
			}
			String path = target.substring(8);
			RivetRequest rreq = authenticateAndParse(target, "POST", null, false, req, res);
			if (rreq == null) return;
			try {
				HashCode hash = HashCode.fromString(hashStr);
				RivetResult rres;
				Temperature temp;
				if (Queries.isMapped(Poolmgr.dataSource, hash)) {
					rres = RivetResult.FOUND;
					temp = Temperature.HOT;
				} else {
					ByteSinkSource bss = null;
					try {
						long len = req.getContentLengthLong();
						if (len == -1 || len > 8192) {
							bss = new FileByteSinkSource(File.createTempFile("jortage-proxy-", ".dat"), true);
						} else {
							bss = new MemoryByteSinkSource();
						}
						OutputStream sinkOut = bss.getSink().openStream();
						HashingOutputStream hos = new HashingOutputStream(Hashing.sha512(), sinkOut);
						// accessing the input stream sends a 100 Continue
						try (InputStream in = req.getInputStream()) {
							ByteStreams.copy(in, hos);
						}
						hos.close();
						HashCode realHash = hos.hash();
						if (!hash.equals(realHash)) {
							jsonError(res, 400, "Hash of body ("+realHash+") did not match hash in query ("+hash+")");
							return;
						}
						Blob blob = Poolmgr.backingBlobStore.blobBuilder(Poolmgr.hashToPath(hash.toString()))
								.payload(bss.getSource())
								.contentLength(bss.getSource().size())
								.contentType(req.getContentType())
								.build();
						long size = bss.getSource().size();
						Poolmgr.backingBlobStore.putBlob(Poolmgr.bucket, blob,
								new PutOptions().setBlobAccess(BlobAccess.PUBLIC_READ).multipart(size > 8192));
						Queries.putPendingBackup(Poolmgr.dataSource, hash);
						Queries.putFilesize(Poolmgr.dataSource, hash, size);
						rres = RivetResult.ADDED;
						temp = Temperature.FREEZING;
					} finally {
						if (bss != null) bss.close();
					}
				}
				Queries.putMap(Poolmgr.dataSource, rreq.identity, path, hash);
				res.setStatus(200);
				JsonObject obj = new JsonObject();
				JsonObject result = new JsonObject();
				result.addProperty("name", rres.name());
				result.addProperty("temperature", temp.name());
				obj.add("result", result);
				sendJson(res, obj);
			} catch (Exception e) {
				jsonExceptionError(res, e, "identity: "+rreq.identity, "target: "+target+(req.getQueryString() == null ? "" : "?"+req.getQueryString()));
				return;
			}
		} else {
			res.sendError(404);
		}
	}

	private RivetRequest authenticateAndParse(String target, String method, String expectedContentType,
			boolean validateAndParseBody, HttpServletRequest req, HttpServletResponse res) throws IOException {
		if (expectedContentType != null)
			expectedContentType = expectedContentType.replace(" ", "").toLowerCase(Locale.ROOT);
		try {
			if ("OPTIONS".equals(req.getMethod())) {
				res.setHeader("Allow", method);
				res.setHeader("Accept", "application/json;charset=utf-8");
				res.setStatus(204);
				res.getOutputStream().close();
				return null;
			}
			if (!method.equals(req.getMethod())) {
				res.setHeader("Allow", method);
				jsonError(res, 405, "Only "+method+" is accepted");
				return null;
			}
			String authHeader = req.getHeader("Rivet-Auth");
			if (authHeader == null) {
				jsonError(res, 401, "Rivet-Auth header missing");
				return null;
			}
			Iterator<String> iter = RIVET_AUTH_SPLITTER.split(authHeader).iterator();
			if (!iter.hasNext()) {
				jsonError(res, 401, "Rivet-Auth header invalid (Not enough fields)");
				return null;
			}
			String identity = iter.next();
			if (!iter.hasNext()) {
				jsonError(res, 401, "Rivet-Auth header invalid (Not enough fields)");
				return null;
			}
			String macStr = iter.next();
			if (!iter.hasNext()) {
				jsonError(res, 401, "Rivet-Auth header invalid (Not enough fields)");
				return null;
			}
			String dateStr = iter.next();
			verify(!iter.hasNext());
			
			Instant date;
			try {
				date = Instant.from(DateTimeFormatter.ISO_INSTANT.parse(dateStr));
			} catch (DateTimeParseException e) {
				jsonError(res, 401, "Rivet-Auth header invalid (Could not parse date)");
				return null;
			}
			if (date.isBefore(Instant.now().minus(5, ChronoUnit.MINUTES))) {
				jsonError(res, 401, "Rivet-Auth header invalid (Too old)");
				return null;
			}
			if (date.isAfter(Instant.now().plus(2, ChronoUnit.MINUTES))) {
				jsonError(res, 401, "Rivet-Auth header invalid (From future)");
				return null;
			}
			
			if (!Poolmgr.users.containsKey(identity)) {
				jsonError(res, 401, "Rivet-Auth header invalid (Bad access ID)");
				return null;
			}
			if (validateAndParseBody) {
				if (req.getContentLength() == -1) {
					jsonError(res, 411, "Length required");
					return null;
				}
				if (req.getContentLength() > 8192) {
					jsonError(res, 413, "Payload too large");
					return null;
				}
			}
			if (expectedContentType != null) {
				String contentType = req.getHeader("Content-Type");
				if (contentType == null || !expectedContentType.equals(contentType.replace(" ", "").toLowerCase(Locale.ROOT))) {
					res.setHeader("Accept", expectedContentType);
					jsonError(res, 415, "Content-Type must be "+expectedContentType);
					return null;
				}
			}
			byte[] theirMac = BaseEncoding.base64().decode(macStr);
			Mac mac = assertSuccess(() -> Mac.getInstance("HmacSHA512"));
			byte[] payload;
			if (validateAndParseBody) {
				payload = ByteStreams.toByteArray(ByteStreams.limit(req.getInputStream(), req.getContentLength()));
				req.getInputStream().close();
			} else {
				payload = new byte[0];
			}
			String payloadStr = new String(payload, Charsets.UTF_8);
			
			String key = Poolmgr.users.get(identity);
			assertSuccess(() -> mac.init(new SecretKeySpec(key.getBytes(Charsets.UTF_8), "RAW")));
			String query;
			if (req.getQueryString() == null) {
				query = "";
			} else {
				query = "?"+req.getQueryString();
			}
			mac.update((target+query+":"+identity+":"+dateStr+":"+payloadStr).getBytes(Charsets.UTF_8));
			byte[] ourMac = mac.doFinal();
			if (!MessageDigest.isEqual(theirMac, ourMac)) {
				jsonError(res, 401, "Rivet-Auth header invalid (Bad MAC)");
				return null;
			}
			
			JsonObject json;
			if (validateAndParseBody) {
				try {
					json = gson.fromJson(payloadStr, JsonObject.class);
				} catch (JsonSyntaxException e) {
					jsonError(res, 400, "Syntax error in payload");
					return null;
				}
			} else {
				json = null;
			}
			return new RivetRequest(identity, json);
		} catch (Throwable t) {
			jsonExceptionError(res, t);
			return null;
		}
	}


	private void jsonExceptionError(HttpServletResponse res, Throwable t, String... extra) throws IOException {
		byte[] tokenBys = new byte[8];
		ThreadLocalRandom.current().nextBytes(tokenBys);
		String token = BaseEncoding.base16().lowerCase().encode(tokenBys);
		System.err.println("== BEGIN "+token+" ==");
		t.printStackTrace();
		if (extra.length > 0) {
			System.err.println("Extra information:");
			for (String s : extra) {
				System.err.println(s);
			}
		}
		System.err.println("== END "+token+" ==");
		jsonError(res, 500, "Internal error "+token);
	}


	private void jsonError(HttpServletResponse res, int code, String msg) throws IOException {
		res.setStatus(code);
		res.setHeader("Content-Type", "application/json; charset=utf-8");
		JsonObject obj = new JsonObject();
		obj.addProperty("error", msg);
		sendJson(res, obj);
	}
	
	private void sendJson(HttpServletResponse res, JsonObject obj) throws IOException {
		res.setHeader("Content-Type", "application/json; charset=utf-8");
		res.getOutputStream().write(obj.toString().getBytes(Charsets.UTF_8));
		res.getOutputStream().close();
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
	
	// https://chromium.googlesource.com/chromium/chromium/+/master/net/base/net_util.cc#92
	private static final ImmutableSet<Integer> illegalPorts = ImmutableSet.of(
			1, // tcpmux
			7, // echo
			9, // discard
			11, // systat
			13, // daytime
			15, // netstat
			17, // qotd
			19, // chargen
			20, // ftp data
			21, // ftp access
			22, // ssh
			23, // telnet
			25, // smtp
			37, // time
			42, // name
			43, // nicname
			53, // domain
			77, // priv-rjs
			79, // finger
			87, // ttylink
			95, // supdup
			101, // hostriame
			102, // iso-tsap
			103, // gppitnp
			104, // acr-nema
			109, // pop2
			110, // pop3
			111, // sunrpc
			113, // auth
			115, // sftp
			117, // uucp-path
			119, // nntp
			123, // NTP
			135, // loc-srv /epmap
			139, // netbios
			143, // imap2
			179, // BGP
			389, // ldap
			465, // smtp+ssl
			512, // print / exec
			513, // login
			514, // shell
			515, // printer
			526, // tempo
			530, // courier
			531, // chat
			532, // netnews
			540, // uucp
			556, // remotefs
			563, // nntp+ssl
			587, // stmp?
			601, // ??
			636, // ldap+ssl
			993, // ldap+ssl
			995, // pop3+ssl
			2049, // nfs
			3659, // apple-sasl / PasswordServer
			4045, // lockd
			6000, // X11
			6665, // Alternate IRC [Apple addition]
			6666, // Alternate IRC [Apple addition]
			6667, // Standard IRC [Apple addition]
			6668, // Alternate IRC [Apple addition]
			6669 // Alternate IRC [Apple addition]
	);
	
}