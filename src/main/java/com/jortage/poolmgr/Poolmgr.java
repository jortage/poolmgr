package com.jortage.poolmgr;

import java.io.File;
import java.lang.reflect.Field;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import sun.misc.Signal;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.gaul.s3proxy.AuthenticationType;
import org.gaul.s3proxy.S3Proxy;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.filesystem.reference.FilesystemConstants;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;

import com.jortage.poolmgr.http.MastodonHackHandler;
import com.jortage.poolmgr.http.OuterHandler;
import com.jortage.poolmgr.http.RedirHandler;
import com.jortage.poolmgr.rivet.RivetHandler;
import com.zaxxer.hikari.HikariDataSource;

import com.google.common.base.MoreObjects;
import com.google.common.base.Stopwatch;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.escape.Escaper;
import com.google.common.hash.HashCode;
import com.google.common.net.UrlEscapers;

import blue.endless.jankson.Jankson;
import blue.endless.jankson.JsonElement;
import blue.endless.jankson.JsonObject;
import blue.endless.jankson.JsonPrimitive;

public class Poolmgr {

	private static final File configFile = new File("config.jkson");
	public static long configFileLastLoaded;
	public static BlobStore backingBlobStore, backingBackupBlobStore, dumpsStore;
	public static String bucket, backupBucket;
	public static String publicHost;
	public static DataSource dataSource;
	public static Map<String, String> users;
	public static volatile boolean readOnly = false;
	private static boolean backingUp = false;
	private static boolean rivetEnabled;
	private static boolean rivetState;
	public static boolean useNewUrls;
	
	public static final Table<String, String, Object> provisionalMaps = HashBasedTable.create();

	public static void main(String[] args) throws Exception {
		try {
			Properties dumpsProps = new Properties();
			dumpsProps.setProperty(FilesystemConstants.PROPERTY_BASEDIR, "dumps");
			dumpsStore = ContextBuilder.newBuilder("filesystem")
					.overrides(dumpsProps)
					.build(BlobStoreContext.class)
					.getBlobStore();
					
			Stopwatch initSw = Stopwatch.createStarted();
			reloadConfig();
	
			System.err.print("Starting S3 server... ");
			System.err.flush();
			S3Proxy s3Proxy = S3Proxy.builder()
					.awsAuthentication(AuthenticationType.AWS_V2_OR_V4, "DUMMY", "DUMMY")
					.endpoint(URI.create("http://localhost:23278"))
					.jettyMaxThreads(24)
					.v4MaxNonChunkedRequestSize(128L*1024L*1024L)
					// S3Proxy will throw if it sees an X-Amz header it doesn't recognize
					// Misskey, starting in some recent version (as of July 2023) now sends an X-Amz-User-Agent header
					// So without this, Misskey instances can't upload files. Cool thanks
					.ignoreUnknownHeaders(true)
					.build();
			
			// excuse me, this is mine now
			Field serverField = S3Proxy.class.getDeclaredField("server");
			serverField.setAccessible(true);
			Server s3ProxyServer = (Server) serverField.get(s3Proxy);
			s3ProxyServer.setHandler(new OuterHandler(new MastodonHackHandler(s3ProxyServer.getHandler())));
			QueuedThreadPool pool = (QueuedThreadPool)s3ProxyServer.getThreadPool();
			pool.setName("Jetty-Common");
	
			s3Proxy.setBlobStoreLocator((identity, container, blob) -> {
				reloadConfigIfChanged();
				String secret = users.get(identity);
				if (secret != null) {
					return Maps.immutableEntry(secret,
							new JortageBlobStore(backingBlobStore, dumpsStore, bucket, identity, dataSource));
				} else {
					throw new RuntimeException("Access denied");
				}
			});
	
			s3Proxy.start();
			System.err.println("ready on http://localhost:23278");
	
			System.err.print("Starting redirector server... ");
			System.err.flush();
			Server redir = new Server(pool);
			ServerConnector redirConn = new ServerConnector(redir);
			redirConn.setHost("localhost");
			redirConn.setPort(23279);
			redir.addConnector(redirConn);
			redir.setHandler(new OuterHandler(new RedirHandler(dumpsStore)));
			redir.start();
			System.err.println("ready on http://localhost:23279");
			
			rivetState = rivetEnabled;
			if (rivetEnabled) {
				System.err.print("Starting Rivet server... ");
				System.err.flush();
				Server rivet = new Server(pool);
				ServerConnector rivetConn = new ServerConnector(rivet);
				rivetConn.setHost("localhost");
				rivetConn.setPort(23280);
				rivet.addConnector(rivetConn);
				rivet.setHandler(new OuterHandler(new RivetHandler()));
				rivet.start();
				System.err.println("ready on http://localhost:23280");
			} else {
				System.err.println("Not starting Rivet server.");
			}
			
			System.err.print("Registering SIGALRM handler for backups... ");
			System.err.flush();
			try {
				Signal.handle(new Signal("ALRM"), (sig) -> {
					reloadConfigIfChanged();
					if (backingUp) {
						System.err.println("Ignoring SIGALRM, backup already in progress");
						return;
					}
					if (backupBucket == null) {
						System.err.println("Ignoring SIGALRM, nowhere to backup to");
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
											if (src == null) {
												Blob actualSrc = backingBackupBlobStore.getBlob(backupBucket, path);
												if (actualSrc == null) {
													System.err.println("Can't find blob "+path+" in source or destination?");
													continue;
												}  else {
													System.err.println("Copying "+path+" from \"backup\" to current - this is a little odd");
													backingBlobStore.putBlob(bucket, actualSrc, new PutOptions().setBlobAccess(BlobAccess.PUBLIC_READ));
												}
											} else {
												backingBackupBlobStore.putBlob(backupBucket, src, new PutOptions().setBlobAccess(BlobAccess.PUBLIC_READ));
											}
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
				System.err.println("done");
			} catch (Exception e) {
				System.err.println("failed");
			}
			System.err.println("This Poolmgr has Super Denim Powers. (Done in "+initSw+")");
		} catch (Throwable t) {
			System.err.println(" failed");
			t.printStackTrace();
		}
	}

	public static void reloadConfigIfChanged() {
//		if (System.currentTimeMillis()-configFileLastLoaded > 500 && configFile.lastModified() > configFileLastLoaded) reloadConfig();
	}

	private static String s(int i) {
		return i == 1 ? "" : "s";
	}

	private static void reloadConfig() {
		boolean reloading = dataSource != null;
		try {
			String prelude = "\r"+(reloading ? "Reloading" : "Loading")+" config: ";
			System.err.print(prelude+"Parsing...");
			System.err.flush();
			JsonObject configTmp = Jankson.builder().build().load(configFile);
			long configFileLastLoadedTmp = System.currentTimeMillis();
			String bucketTmp = ((JsonPrimitive)configTmp.getObject("backend").get("bucket")).asString();
			String publicHostTmp = ((JsonPrimitive)configTmp.getObject("backend").get("publicHost")).asString();
			boolean rivetEnabledTmp = configTmp.recursiveGet(boolean.class, "rivet.enabled");
			boolean readOnlyTmp = MoreObjects.firstNonNull(configTmp.get(boolean.class, "readOnly"), false);
			boolean useNewUrlsTmp = MoreObjects.firstNonNull(configTmp.get(boolean.class, "useNewUrls"), false);
			System.err.print(prelude+"Constructing blob stores...");
			System.err.flush();
			BlobStore backingBlobStoreTmp = createBlobStore(configTmp.getObject("backend"));
			String backupBucketTmp;
			BlobStore backingBackupBlobStoreTmp;
			if (configTmp.containsKey("backupBackend")) {
				backupBucketTmp = ((JsonPrimitive)configTmp.getObject("backupBackend").get("bucket")).asString();
				backingBackupBlobStoreTmp = createBlobStore(configTmp.getObject("backupBackend"));
			} else {
				backupBucketTmp = null;
				backingBackupBlobStoreTmp = null;
			}
			JsonObject sql = configTmp.getObject("mysql");
			String sqlHost = ((JsonPrimitive)sql.get("host")).asString();
			int sqlPort = ((Number)((JsonPrimitive)sql.get("port")).getValue()).intValue();
			String sqlDb = ((JsonPrimitive)sql.get("database")).asString();
			String sqlUser = ((JsonPrimitive)sql.get("user")).asString();
			String sqlPass = ((JsonPrimitive)sql.get("pass")).asString();
			Escaper pesc = UrlEscapers.urlPathSegmentEscaper();
			Escaper esc = UrlEscapers.urlFormParameterEscaper();
			System.err.print(prelude+"Connecting to MariaDB...   ");
			System.err.flush();
			HikariDataSource dataSourceTmp = new HikariDataSource();
			dataSourceTmp.setJdbcUrl("jdbc:mariadb://"+pesc.escape(sqlHost)+":"+sqlPort+"/"+pesc.escape(sqlDb));
			dataSourceTmp.setUsername(sqlUser);
			dataSourceTmp.setPassword(sqlPass);
			dataSourceTmp.addDataSourceProperty("cachePrepStmts", "true");
			dataSourceTmp.addDataSourceProperty("prepStmtCacheSize", "100");
			dataSourceTmp.addDataSourceProperty("prepStmtCacheSqlLimit", "3000");
			dataSourceTmp.addDataSourceProperty("useServerPrepStmts", "true");
			dataSourceTmp.addDataSourceProperty("useLocalSessionState", "true");
			dataSourceTmp.addDataSourceProperty("rewriteBatchedStatements", "true");
			dataSourceTmp.addDataSourceProperty("cacheResultSetMetadata", "true");
			dataSourceTmp.addDataSourceProperty("cacheServerConfiguration", "true");
			dataSourceTmp.addDataSourceProperty("elideSetAutoCommits", "true");
			dataSourceTmp.addDataSourceProperty("maintainTimeStats", "false");
			try (Connection c = dataSourceTmp.getConnection()) {
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
			ImmutableMap.Builder<String, String> usersTmp = ImmutableMap.builder();
			for (Map.Entry<String, JsonElement> en : configTmp.getObject("users").entrySet()) {
				usersTmp.put(en.getKey(), ((JsonPrimitive)en.getValue()).asString());
				dumpsStore.createContainerInLocation(null, en.getKey());
			}
			users = usersTmp.build();
			System.err.println("\r"+(reloading ? "Reloading" : "Loading")+" config... done                  ");
			HikariDataSource oldDataSource = (HikariDataSource)dataSource;
			configFileLastLoaded = configFileLastLoadedTmp;
			bucket = bucketTmp;
			publicHost = publicHostTmp;
			backingBlobStore = backingBlobStoreTmp;
			backupBucket = backupBucketTmp;
			backingBackupBlobStore = backingBackupBlobStoreTmp;
			dataSource = dataSourceTmp;
			rivetEnabled = rivetEnabledTmp;
			useNewUrls = useNewUrlsTmp;
			if (rivetState != rivetEnabled && reloading) {
				System.err.println("WARNING: Cannot hot-"+(rivetEnabled ? "enable" : "disable")+" Rivet. jortage-proxy must be restarted for this change to take effect.");
			}
			readOnly = readOnlyTmp;
			if (oldDataSource != null) {
				oldDataSource.close();
			}
		} catch (Exception e) {
			System.err.println(" failed");
			e.printStackTrace();
			if (reloading) {
				System.err.println("Failed to reload config. Behavior unchanged.");
			} else {
				System.err.println("Failed to load config. Exit");
				System.exit(2);
			}
		}
	}

	private static BlobStore createBlobStore(JsonObject obj) {
		return ContextBuilder.newBuilder("aws-s3")
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

	public static void checkReadOnly() {
		if (readOnly) throw new IllegalStateException("Currently in read-only maintenance mode; try again later");
	}

}
