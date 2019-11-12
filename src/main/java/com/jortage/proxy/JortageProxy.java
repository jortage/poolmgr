package com.jortage.proxy;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import sun.misc.Signal;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.gaul.s3proxy.AuthenticationType;
import org.gaul.s3proxy.BlobStoreLocator;
import org.gaul.s3proxy.S3Proxy;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.filesystem.reference.FilesystemConstants;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.mariadb.jdbc.MariaDbPoolDataSource;

import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.escape.Escaper;
import com.google.common.hash.HashCode;
import com.google.common.io.ByteStreams;
import com.google.common.net.UrlEscapers;

import blue.endless.jankson.Jankson;
import blue.endless.jankson.JsonObject;
import blue.endless.jankson.JsonPrimitive;

public class JortageProxy {

	private static final Splitter SPLITTER = Splitter.on('/').limit(2).omitEmptyStrings();

	private static final File configFile = new File("config.jkson");
	private static JsonObject config;
	private static long configFileLastLoaded;
	private static BlobStore backingBlobStore;
	private static BlobStore backingBackupBlobStore;
	private static String bucket;
	private static String backupBucket;
	private static String publicHost;
	private static MariaDbPoolDataSource dataSource;
	private static boolean backingUp = false;

	@SuppressWarnings("restriction")
	public static void main(String[] args) throws Exception {
		reloadConfig();

		S3Proxy s3Proxy = S3Proxy.builder()
				.awsAuthentication(AuthenticationType.AWS_V2_OR_V4, "DUMMY", "DUMMY")
				.endpoint(URI.create("http://localhost:23278"))
				.jettyMaxThreads(24)
				.v4MaxNonChunkedRequestSize(128L*1024L*1024L)
				.build();

		Properties dumpsProps = new Properties();
		dumpsProps.setProperty(FilesystemConstants.PROPERTY_BASEDIR, "dumps");
		BlobStore dumpsStore = ContextBuilder.newBuilder("filesystem")
				.overrides(dumpsProps)
				.build(BlobStoreContext.class)
				.getBlobStore();

		s3Proxy.setBlobStoreLocator(new BlobStoreLocator() {

			@Override
			public Entry<String, BlobStore> locateBlobStore(String identity, String container, String blob) {
				reloadConfigIfChanged();
				if (config.containsKey("users") && config.getObject("users").containsKey(identity)) {
					return Maps.immutableEntry(((JsonPrimitive)config.getObject("users").get(identity)).asString(), new JortageBlobStore(backingBlobStore, dumpsStore, bucket, identity, dataSource));
				} else {
					throw new RuntimeException("Access denied");
				}
			}
		});

		s3Proxy.start();
		System.err.println("S3 listening on localhost:23278");

		QueuedThreadPool pool = new QueuedThreadPool(24);
		pool.setName("Redir-Jetty");
		Server redir = new Server(pool);
		ServerConnector conn = new ServerConnector(redir);
		conn.setHost("localhost");
		conn.setPort(23279);
		redir.addConnector(conn);
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
					if (name.startsWith("backups/dumps") || name.startsWith("/backups/dumps")) {
						Blob b = dumpsStore.getBlob(identity, name);
						if (b != null) {
							response.setHeader("Cache-Control", "private, no-cache");
							response.setHeader("Content-Type", b.getMetadata().getContentMetadata().getContentType());
							response.setStatus(200);
							ByteStreams.copy(b.getPayload().openStream(), response.getOutputStream());
						} else {
							response.sendError(404);
						}
						return;
					}
					try {
						String hash = Queries.getMap(dataSource, identity, name).toString();
						response.setHeader("Cache-Control", "public");
						response.setHeader("Location", publicHost+"/"+hashToPath(hash));
						response.setStatus(301);
					} catch (IllegalArgumentException e) {
						response.sendError(404);
					}
				}
			}
		});
		redir.start();
		System.err.println("Redirector listening on localhost:23279");
		Signal.handle(new Signal("ALRM"), (sig) -> {
			reloadConfigIfChanged();
			if (backingUp) {
				System.err.println("Ignoring SIGALRM, backup already in progress");
				return;
			}
			new Thread(() -> {
				int count = 0;
				Stopwatch sw = Stopwatch.createStarted();
				try (Connection c = dataSource.getConnection()) {
					backingUp = true;
					try (PreparedStatement delete = c.prepareStatement("DELETE FROM `pending_backup` WHERE `hash` = ?;")) {
						try (PreparedStatement ps = c.prepareStatement("SELECT `hash` FROM `pending_backup`;")) {
							try (ResultSet rs = ps.executeQuery()) {
								while (rs.next()) {
									byte[] bys = rs.getBytes("hash");
									String path = hashToPath(HashCode.fromBytes(bys).toString());
									Blob src = backingBlobStore.getBlob(bucket, path);
									backingBackupBlobStore.putBlob(backupBucket, src);
									delete.setBytes(1, bys);
									delete.executeUpdate();
									count++;
								}
								System.err.println("Backup of "+count+" item"+s(count)+" successful in "+sw);
							}
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
					System.err.println("Backup failed after "+count+" item"+s(count)+" in "+sw);
				} finally {
					backingUp = false;
				}
			}, "Backup thread").start();
		});
	}

	private static void reloadConfigIfChanged() {
		if (System.currentTimeMillis()-configFileLastLoaded > 500 && configFile.lastModified() > configFileLastLoaded) reloadConfig();
	}

	private static String s(int i) {
		return i == 1 ? "" : "s";
	}

	private static void reloadConfig() {
		try {
			config = Jankson.builder().build().load(configFile);
			configFileLastLoaded = System.currentTimeMillis();
			bucket = ((JsonPrimitive)config.getObject("backend").get("bucket")).asString();
			backupBucket = ((JsonPrimitive)config.getObject("backupBackend").get("bucket")).asString();
			publicHost = ((JsonPrimitive)config.getObject("backend").get("publicHost")).asString();
			backingBlobStore = createBlobStore("backend");
			backingBackupBlobStore = createBlobStore("backupBackend");
			JsonObject sql = config.getObject("mysql");
			String sqlHost = ((JsonPrimitive)sql.get("host")).asString();
			int sqlPort = ((Number)((JsonPrimitive)sql.get("port")).getValue()).intValue();
			String sqlDb = ((JsonPrimitive)sql.get("database")).asString();
			String sqlUser = ((JsonPrimitive)sql.get("user")).asString();
			String sqlPass = ((JsonPrimitive)sql.get("pass")).asString();
			Escaper pesc = UrlEscapers.urlPathSegmentEscaper();
			Escaper esc = UrlEscapers.urlFormParameterEscaper();
			if (dataSource != null) {
				dataSource.close();
			}
			dataSource = new MariaDbPoolDataSource("jdbc:mariadb://"+pesc.escape(sqlHost)+":"+sqlPort+"/"+pesc.escape(sqlDb)+"?user="+esc.escape(sqlUser)+"&password="+esc.escape(sqlPass)+"&autoReconnect=true");
			try (Connection c = dataSource.getConnection()) {
				execOneshot(c, "CREATE TABLE IF NOT EXISTS `name_map` (\n" +
						"  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,\n" +
						"  `identity` VARCHAR(255) NOT NULL,\n" +
						"  `name` VARCHAR(255) NOT NULL,\n" +
						"  `hash` BINARY(64) NOT NULL,\n" +
						"  PRIMARY KEY (`id`),\n" +
						"  UNIQUE INDEX `forward` (`identity`, `name`),\n" +
						"  INDEX `reverse` (`hash`)\n" +
						") ROW_FORMAT=COMPRESSED;");
				execOneshot(c, "CREATE TABLE IF NOT EXISTS `multipart_uploads` (\n" +
						"  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,\n" +
						"  `identity` VARCHAR(255) NOT NULL,\n" +
						"  `name` VARCHAR(255) NOT NULL,\n" +
						"  `tempfile` VARCHAR(255) NOT NULL,\n" +
						"  PRIMARY KEY (`id`),\n" +
						"  UNIQUE INDEX `forward` (`identity`, `name`),\n" +
						"  UNIQUE INDEX `reverse` (`tempfile`)\n" +
						") ROW_FORMAT=COMPRESSED;");
				execOneshot(c, "CREATE TABLE IF NOT EXISTS `filesizes` (\n" +
						"  `hash` BINARY(64) NOT NULL,\n" +
						"  `size` BIGINT UNSIGNED NOT NULL,\n" +
						"  PRIMARY KEY (`hash`)\n" +
						") ROW_FORMAT=COMPRESSED;");
				execOneshot(c, "CREATE TABLE IF NOT EXISTS `pending_backup` (\n" +
						"  `hash` BINARY(64) NOT NULL,\n" +
						"  PRIMARY KEY (`hash`)\n" +
						") ROW_FORMAT=COMPRESSED;");
			}
			System.err.println("Config file reloaded.");
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Failed to reload config. Behavior unchanged.");
		}
	}

	private static BlobStore createBlobStore(String string) {
		JsonObject obj = config.getObject(string);
		return ContextBuilder.newBuilder("s3")
			.credentials(((JsonPrimitive)obj.get("accessKeyId")).asString(), ((JsonPrimitive)obj.get("secretAccessKey")).asString())
			.modules(ImmutableList.of(new SLF4JLoggingModule()))
			.endpoint(((JsonPrimitive)obj.get("endpoint")).asString())
			.build(BlobStoreContext.class)
			.getBlobStore();
	}

	private static void execOneshot(Connection c, String sql) throws SQLException {
		try (Statement s = c.createStatement()) {
			s.execute(sql);
		}
	}

	public static String hashToPath(String hash) {
		return "blobs/"+hash.substring(0, 1)+"/"+hash.substring(1, 4)+"/"+hash;
	}

}
