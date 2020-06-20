package com.jortage.poolmgr;

import java.io.File;

import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;

public class FileByteSinkSource implements ByteSinkSource {

	private final File file;
	private final boolean deleteOnClose;

	public FileByteSinkSource(File file, boolean deleteOnClose) {
		this.file = file;
		this.deleteOnClose = deleteOnClose;
	}
	
	@Override
	public ByteSink getSink() {
		return Files.asByteSink(file);
	}
	
	@Override
	public ByteSource getSource() {
		return Files.asByteSource(file);
	}
	
	@Override
	public void close() {
		if (deleteOnClose) {
			file.delete();
		}
	}
	
}
