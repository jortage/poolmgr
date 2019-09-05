package com.jortage.proxy;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.gaul.s3proxy.AuthenticationType;
import org.gaul.s3proxy.BlobStoreLocator;
import org.gaul.s3proxy.S3Proxy;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.type.StringDataType;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import blue.endless.jankson.Jankson;
import blue.endless.jankson.JsonObject;
import blue.endless.jankson.JsonPrimitive;

public class JortageProxy {

	private static final Splitter SPLITTER = Splitter.on('/').limit(3).omitEmptyStrings();

	private static final File configFile = new File("config.jkson");
	private static JsonObject config;
	private static long configFileLastLoaded;
	private static BlobStore backingBlobStore;
	private static String bucket;
	private static String publicHost;

	public static void main(String[] args) throws Exception {
		reloadConfig();

		MVStore store = new MVStore.Builder()
				.fileName("data.mv")
				.compress()
				.open();
		MVMap<String, String> paths = store.openMap("paths", new MVMap.Builder<String, String>()
				.keyType(new StringDataType())
				.valueType(new StringDataType()));

		S3Proxy s3Proxy = S3Proxy.builder()
				.awsAuthentication(AuthenticationType.AWS_V4, "DUMMY", "DUMMY")
				.endpoint(URI.create("http://localhost:23278"))
				.build();

		s3Proxy.setBlobStoreLocator(new BlobStoreLocator() {

			@Override
			public Entry<String, BlobStore> locateBlobStore(String identity, String container, String blob) {
				System.out.println("identity: "+identity);
				if (System.currentTimeMillis()-configFileLastLoaded > 500 && configFile.lastModified() > configFileLastLoaded) reloadConfig();
				if (config.containsKey("users") && config.getObject("users").containsKey(identity)) {
					return Maps.immutableEntry(((JsonPrimitive)config.getObject("users").get(identity)).asString(), new JortageBlobStore(backingBlobStore, bucket, identity, paths));
				} else {
					throw new RuntimeException("Access denied");
				}
			}
		});

		s3Proxy.start();
		System.err.println("S3 listening on localhost:23278");

		Server redir = new Server(new InetSocketAddress("localhost", 23279));
		redir.setHandler(new AbstractHandler() {

			@Override
			public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
				baseRequest.setHandled(true);
				if ("/".equals(target) || "/index.html".equals(target) || "".equals(target)) {
					response.setHeader("Location", "https://jortage.com");
					response.setStatus(301);
					return;
				}
				List<String> split = SPLITTER.splitToList(target);
				if (split.size() != 3) {
					response.sendError(400);
					return;
				} else {
					for (Map.Entry<String, String> en : paths.entrySet()) {
						System.out.println(en);
					}
					String identity = split.get(0);
					String container = split.get(1);
					String name = split.get(2);
					String key = buildKey(identity, container, name);
					if (paths.containsKey(key)) {
						response.setHeader("Location", publicHost+"/"+hashToPath(paths.get(key)));
						response.setStatus(301);
						return;
					} else {
						response.sendError(404);
						return;
					}
				}
			}
		});
		redir.start();
		System.err.println("Redirector listening on localhost:23279");

		while (true) {
			Thread.sleep(15000);
			store.commit();
		}
	}

	private static void reloadConfig() {
		try {
			config = Jankson.builder().build().load(configFile);
			configFileLastLoaded = System.currentTimeMillis();
			bucket = ((JsonPrimitive)config.getObject("backend").get("bucket")).asString();
			publicHost = ((JsonPrimitive)config.getObject("backend").get("publicHost")).asString();
			Properties props = new Properties();
			props.put("jclouds.wire", "debug");
			backingBlobStore = ContextBuilder.newBuilder("s3")
					.credentials(((JsonPrimitive)config.getObject("backend").get("accessKeyId")).asString(), ((JsonPrimitive)config.getObject("backend").get("secretAccessKey")).asString())
					.modules(ImmutableList.of(new SLF4JLoggingModule()))
					.endpoint(((JsonPrimitive)config.getObject("backend").get("endpoint")).asString())
					.overrides(props)
					.build(BlobStoreContext.class)
					.getBlobStore();
			System.err.println("Config file reloaded.");
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Failed to reload config. The config is not changed.");
		}
	}

	public static String buildKey(String identity, String container, String name) {
		return identity+":"+container+":"+name;
	}

	public static String hashToPath(String hash) {
		return "blobs/"+hash.substring(0, 1)+"/"+hash.substring(1, 4)+"/"+hash;
	}

}
