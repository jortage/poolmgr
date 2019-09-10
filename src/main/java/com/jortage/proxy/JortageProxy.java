package com.jortage.proxy;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

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

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;

import blue.endless.jankson.Jankson;
import blue.endless.jankson.JsonObject;
import blue.endless.jankson.JsonPrimitive;

public class JortageProxy {

	private static final Splitter SPLITTER = Splitter.on('/').limit(2).omitEmptyStrings();

	private static final File configFile = new File("config.jkson");
	private static JsonObject config;
	private static long configFileLastLoaded;
	private static BlobStore backingBlobStore;
	private static String bucket;
	private static String publicHost;

	public static void main(String[] args) throws Exception {
		reloadConfig();

		File restore = new File("data.dat.gz");
		File mv = new File("data.mv");
		boolean doRestore = false;
		if (!mv.exists() && restore.exists()) {
			doRestore = true;
		}

		MVStore store = new MVStore.Builder()
				.fileName("data.mv")
				.compress()
				.open();
		MVMap<String, String> paths = store.openMap("paths", new MVMap.Builder<String, String>()
				.keyType(new StringDataType())
				.valueType(new StringDataType()));
		if (doRestore) {
			int recovered = 0;
			int total = 0;
			boolean success = false;
			try (DataInputStream dis = new DataInputStream(new GZIPInputStream(new FileInputStream(restore)))) {
				int count = dis.readInt();
				total = count;
				for (int i = 0; i < count; i++) {
					byte[] key = new byte[dis.readInt()];
					dis.readFully(key);
					byte[] val = new byte[dis.readInt()];
					dis.readFully(val);
					String keyStr = new String(key, Charsets.UTF_8);
					String valStr = new String(val, Charsets.UTF_8);
					paths.put(keyStr, valStr);
					recovered++;
				}
				System.err.println("Recovery successful! Recovered "+recovered+" entries.");
				success = true;
			} catch (IOException e) {
				e.printStackTrace();
				if (recovered == 0) {
					System.err.println("Recovery failed! No data could be restored!");
				} else {
					System.err.println("Recovery failed; only "+recovered+"/"+total+" entries could be restored!");
				}
			}
			if (success) {
				restore.delete();
			}
		}

		S3Proxy s3Proxy = S3Proxy.builder()
				.awsAuthentication(AuthenticationType.AWS_V2_OR_V4, "DUMMY", "DUMMY")
				.endpoint(URI.create("http://localhost:23278"))
				.v4MaxNonChunkedRequestSize(128*1024*1024)
				.build();

		s3Proxy.setBlobStoreLocator(new BlobStoreLocator() {

			@Override
			public Entry<String, BlobStore> locateBlobStore(String identity, String container, String blob) {
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
				if (split.size() != 2) {
					response.sendError(400);
					return;
				} else {
					String identity = split.get(0);
					String name = split.get(1);
					String key = buildKey(identity, name);
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

		SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-DD_HH-mm-ss");

		int i = 0;
		while (true) {
			Thread.sleep(15000);
			store.commit();
			i++;
			// every 10 minutes (roughly)
			// FIXME this is causing OOMEs in production
			if (false && i % 40 == 0) {
				System.err.println("Creating backup...");
				File backups = new File("backups");
				if (!backups.exists()) {
					backups.mkdir();
					Files.write(
						  "These are gzipped dumps of the contents of `data.mv` in an ad-hoc format\n"
						+ "that can be restored by jortage-proxy in the event of data loss. To cause\n"
						+ "such a restore, delete `data.mv` and copy one of these files to `data.dat.gz`\n"
						+ "in the jortage-proxy directory. Upon start, jortage-proxy will load the\n"
						+ "data in the dump into the newly-created `data.mv` and then delete `data.dat.gz`.",
						new File(backups, "README.txt"), Charsets.UTF_8);
				}
				File f = new File("backups/"+dateFormat.format(new Date())+".dat.gz");
				try (DataOutputStream dos = new DataOutputStream(new GZIPOutputStream(new FileOutputStream(f)))) {
					List<Map.Entry<String, String>> entries = Lists.newArrayList(paths.entrySet());
					dos.writeInt(entries.size());
					for (Map.Entry<String, String> en : entries) {
						byte[] keyBys = en.getKey().getBytes(Charsets.UTF_8);
						byte[] valBys = en.getValue().getBytes(Charsets.UTF_8);
						dos.writeInt(keyBys.length);
						dos.write(keyBys);
						dos.writeInt(valBys.length);
						dos.write(valBys);
					}
					System.err.println("Backup successful.");
				} catch (IOException e) {
					e.printStackTrace();
					System.err.println("Failed to write backup!");
				}
			}
		}
	}

	private static void reloadConfig() {
		try {
			config = Jankson.builder().build().load(configFile);
			configFileLastLoaded = System.currentTimeMillis();
			bucket = ((JsonPrimitive)config.getObject("backend").get("bucket")).asString();
			publicHost = ((JsonPrimitive)config.getObject("backend").get("publicHost")).asString();
			Properties props = new Properties();
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

	public static String buildKey(String identity, String name) {
		return identity+":"+name;
	}

	public static String hashToPath(String hash) {
		return "blobs/"+hash.substring(0, 1)+"/"+hash.substring(1, 4)+"/"+hash;
	}

}
