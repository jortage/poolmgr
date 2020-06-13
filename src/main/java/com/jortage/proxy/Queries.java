package com.jortage.proxy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

public class Queries {

	/**
	 * Convert a potential LFN (Long File Name, >255 chars) to a SFN (Short File Name, <=255 chars),
	 * by truncating the LFN and appending a SHA-256 hash and remainder length to the end.
	 * <p>
	 * All names that will be touching the DB must go through here. All the other public methods in
	 * Queries will handle this for you.
	 * <p>
	 * XXX If the SHA-256 hash collides (VERY UNLIKELY!) then the conflicting names will be
	 * entangled; we already depend on the collision resistance of SHA-2 for other (more
	 * important) things, so this is not a huge problem... but, UGH. Very Bad Hack.
	 * <p>
	 * (Why do this at all? You can't index on a TEXT column in MySQL, and VARCHAR only goes
	 * up to 255. A VARCHAR that is >255 is secretly a TEXT.)
	 */
	public static String toSFN(String lfn) {
		if (lfn.length() <= 255) return lfn;
		int remainder = lfn.length()-255;
		String remainderStr = Integer.toString(remainder);
		int remainderLength = remainderStr.length();
		String hash = Hashing.sha256().hashString(lfn, Charsets.UTF_8).toString();
		String sfn = lfn.substring(0, 255-1-64-1-remainderLength)+"~"+hash+"$"+remainderStr;
		return sfn;
	}
	
	public static HashCode getMap(DataSource dataSource, String identity, String name) {
		name = toSFN(name);
		try (Connection c = dataSource.getConnection()) {
			try (PreparedStatement ps = c.prepareStatement("SELECT `hash` FROM `name_map` WHERE `identity` = ? AND `name` = ?;")) {
				ps.setString(1, identity);
				ps.setString(2, name);
				try (ResultSet rs = ps.executeQuery()) {
					if (rs.first()) {
						return HashCode.fromBytes(rs.getBytes("hash"));
					} else {
						throw new IllegalArgumentException("Not found");
					}
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public static void putMap(DataSource dataSource, String identity, String name, HashCode hash) {
		name = toSFN(name);
		try (Connection c = dataSource.getConnection()) {
			try (PreparedStatement ps = c.prepareStatement("INSERT INTO `name_map` (`identity`, `name`, `hash`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `hash` = ?;")) {
				ps.setString(1, identity);
				ps.setString(2, name);
				ps.setBytes(3, hash.asBytes());
				ps.setBytes(4, hash.asBytes());
				ps.executeUpdate();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static boolean deleteMap(DataSource dataSource, String identity, String name) {
		name = toSFN(name);
		try (Connection c = dataSource.getConnection()) {
			try (PreparedStatement ps = c.prepareStatement("DELETE FROM `name_map` WHERE `identity` = ? AND `name` = ?;")) {
				ps.setString(1, identity);
				ps.setString(2, name);
				return ps.executeUpdate() > 0;
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static int getReferenceCount(DataSource dataSource, HashCode hash) {
		try (Connection c = dataSource.getConnection()) {
			try (PreparedStatement ps = c.prepareStatement("SELECT COUNT(1) AS count FROM `name_map` WHERE `hash` = ?;")) {
				ps.setBytes(1, hash.asBytes());
				try (ResultSet rs = ps.executeQuery()) {
					if (rs.first()) {
						return rs.getInt("count");
					} else {
						return 0;
					}
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public static void putFilesize(DataSource dataSource, HashCode hash, long size) {
		try (Connection c = dataSource.getConnection()) {
			try (PreparedStatement ps = c.prepareStatement("INSERT IGNORE INTO `filesizes` (`hash`, `size`) VALUES (?, ?);")) {
				ps.setBytes(1, hash.asBytes());
				ps.setLong(2, size);
				ps.executeUpdate();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public static void putPendingBackup(DataSource dataSource, HashCode hash) {
		try (Connection c = dataSource.getConnection()) {
			try (PreparedStatement ps = c.prepareStatement("INSERT IGNORE INTO `pending_backup` (`hash`) VALUES (?);")) {
				ps.setBytes(1, hash.asBytes());
				ps.executeUpdate();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public static void putMultipart(DataSource dataSource, String identity, String name, String tempfile) {
		name = toSFN(name);
		try (Connection c = dataSource.getConnection()) {
			try (PreparedStatement ps = c.prepareStatement("INSERT INTO `multipart_uploads` (`identity`, `name`, `tempfile`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `tempfile` = ?;")) {
				ps.setString(1, identity);
				ps.setString(2, name);
				ps.setString(3, tempfile);
				ps.setString(4, tempfile);
				ps.executeUpdate();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public static String getMultipart(DataSource dataSource, String identity, String name) {
		name = toSFN(name);
		try (Connection c = dataSource.getConnection()) {
			try (PreparedStatement ps = c.prepareStatement("SELECT `tempfile` FROM `multipart_uploads` WHERE `identity` = ? AND `name` = ?;")) {
				ps.setString(1, identity);
				ps.setString(2, name);
				try (ResultSet rs = ps.executeQuery()) {
					if (rs.first()) {
						return rs.getString("tempfile");
					} else {
						throw new IllegalArgumentException("Not found");
					}
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public static String getMultipartRev(DataSource dataSource, String tempfile) {
		try (Connection c = dataSource.getConnection()) {
			try (PreparedStatement ps = c.prepareStatement("SELECT `name` FROM `multipart_uploads` WHERE `tempfile` = ?;")) {
				ps.setString(1, tempfile);
				try (ResultSet rs = ps.executeQuery()) {
					if (rs.first()) {
						return rs.getString("name");
					} else {
						throw new IllegalArgumentException("Not found");
					}
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public static void removeMultipart(DataSource dataSource, String tempfile) {
		try (Connection c = dataSource.getConnection()) {
			try (PreparedStatement ps = c.prepareStatement("DELETE FROM `multipart_uploads` WHERE `tempfile` = ?;")) {
				ps.setString(1, tempfile);
				ps.executeUpdate();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

}
