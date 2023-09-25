package com.jortage.poolmgr.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Locale;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;

public class PngSurgeon implements Closeable {
	
	public static class CRCException extends IOException {
		public CRCException(String msg) { super(msg); }
	}
	
	public static final class Chunk {
		public static final int IHDR = fourcc("IHDR");
		public static final int PLTE = fourcc("PLTE");
		public static final int IDAT = fourcc("IDAT");
		public static final int IEND = fourcc("IEND");
		public static final int tRNS = fourcc("tRNS");
		public static final int cHRM = fourcc("cHRM");
		public static final int gAMA = fourcc("gAMA");
		public static final int iCCP = fourcc("iCCP");
		public static final int sBIT = fourcc("sBIT");
		public static final int sRGB = fourcc("sRGB");
		public static final int cICP = fourcc("cICP");
		public static final int mDCv = fourcc("mDCv");
		public static final int cLLi = fourcc("cLLi");
		public static final int tEXt = fourcc("tEXt");
		public static final int zTXt = fourcc("zTXt");
		public static final int iTXt = fourcc("iTXt");
		public static final int bKGD = fourcc("bKGD");
		public static final int hIST = fourcc("hIST");
		public static final int pHYs = fourcc("pHYs");
		public static final int sPLT = fourcc("sPLT");
		public static final int eXIf = fourcc("eXIf");
		public static final int tIME = fourcc("tIME");
		public static final int acTL = fourcc("acTL");
		public static final int fcTL = fourcc("fcTL");
		public static final int fdAT = fourcc("fdAT");
	}
	
	public static final long PNG_MAGIC = 0x89504E470D0A1A0AL;
	
	private final DataInputStream in;
	private final DataOutputStream out, crcOut;
	private final CRC32 crc = new CRC32();
	
	private int chunkLength = -1;
	private int chunkType;
	
	public PngSurgeon(InputStream in, OutputStream out) throws IOException {
		this.in = new DataInputStream(new BufferedInputStream(in));
		OutputStream bout = new BufferedOutputStream(out);
		this.out = new DataOutputStream(bout);
		this.crcOut = new DataOutputStream(new CheckedOutputStream(bout, crc));
	}

	public int readChunkType() throws IOException {
		if (chunkLength != -1) throw new IllegalStateException("Current chunk has not been processed");
		chunkLength = in.readInt();
		chunkType = in.readInt();
		return chunkType;
	}
	
	public int getChunkLength() {
		if (chunkLength == -1) throw new IllegalStateException("Data has already been read or no chunk has been read yet");
		return chunkLength;
	}
	
	public byte[] readChunkData() throws IOException {
		if (chunkLength == -1) throw new IllegalStateException("Data has already been read or no chunk has been read yet");
		byte[] data = new byte[chunkLength];
		chunkLength = -1;
		in.readFully(data);
		crc.reset();
		crc.update(Ints.toByteArray(chunkType));
		crc.update(data);
		int actual = in.readInt();
		int expected = (int)crc.getValue();
		if (actual != expected) {
			throw new CRCException("Bad CRC ("+toHexString(actual)+" != "+toHexString(expected)+")");
		}
		return data;
	}

	public void skipChunkData() throws IOException {
		if (chunkLength == -1) throw new IllegalStateException("Data has already been read or no chunk has been read yet");
		in.skipBytes(chunkLength+4);
		chunkLength = -1;
	}
	
	public void copyChunk() throws IOException {
		if (chunkLength == -1) throw new IllegalStateException("Data has already been read or no chunk has been read yet");
		out.writeInt(chunkLength);
		out.writeInt(chunkType);
		ByteStreams.limit(in, chunkLength+4).transferTo(out);
		chunkLength = -1;
	}
	
	public void writeChunk(int chunkType, byte[] data) throws IOException {
		out.writeInt(data.length);
		crc.reset();
		crcOut.writeInt(chunkType);
		crcOut.write(data);
		out.writeInt((int)crc.getValue());
	}
	
	public void writeChunk(int chunkType, ByteArrayOutputStream data) throws IOException {
		out.writeInt(data.size());
		crc.reset();
		crcOut.writeInt(chunkType);
		data.writeTo(crcOut);
		out.writeInt((int)crc.getValue());
	}
	
	public void writeEmptyChunk(int chunkType) throws IOException {
		out.writeInt(0);
		crc.reset();
		crcOut.writeInt(chunkType);
		out.writeInt((int)crc.getValue());
	}
	
	@Override
	public void close() throws IOException {
		in.close();
		out.close();
	}

	private static int fourcc(String str) {
		return Ints.fromByteArray(str.getBytes(Charsets.ISO_8859_1));
	}

	private static String toHexString(int i) {
		return Long.toHexString(((i)&0xFFFFFFFFL)|0xF00000000L).substring(1).toUpperCase(Locale.ROOT);
	}
	
}
