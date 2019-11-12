package com.jortage.proxy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.google.common.hash.HashCode;

public class Queries {

	public static HashCode getMap(DataSource dataSource, String identity, String name) {
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
