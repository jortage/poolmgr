package com.jortage.poolmgr;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;

public class MemoryByteSinkSource implements ByteSinkSource {

	private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
	
	public MemoryByteSinkSource() {}
	public MemoryByteSinkSource(byte[] bys) {
		this(bys, 0, bys.length);
	}
	public MemoryByteSinkSource(byte[] bys, int ofs, int len) {
		baos.write(bys, ofs, len);
	}
	
	@Override
	public ByteSink getSink() {
		return new ByteSink() {
			@Override
			public OutputStream openStream() throws IOException {
				baos.reset();
				return baos;
			}
		};
	}
	
	@Override
	public ByteSource getSource() {
		return new ByteSource() {
			@Override
			public InputStream openStream() throws IOException {
				return new ByteArrayInputStream(baos.toByteArray());
			}
			@Override
			public InputStream openBufferedStream() throws IOException {
				return openStream();
			}
			@Override
			public byte[] read() throws IOException {
				return baos.toByteArray();
			}
			@Override
			public long size() throws IOException {
				return baos.size();
			}
		};
	}
	
	@Override
	public void close() {
		baos.reset();
	}
	
}
