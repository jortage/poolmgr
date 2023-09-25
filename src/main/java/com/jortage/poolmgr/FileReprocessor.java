package com.jortage.poolmgr;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.jortage.poolmgr.util.PngSurgeon;
import com.jortage.poolmgr.util.PngSurgeon.CRCException;
import com.jortage.poolmgr.util.PngSurgeon.Chunk;

import com.google.common.base.Charsets;
import com.google.common.primitives.Longs;

public class FileReprocessor {

	public static void reprocess(InputStream in, OutputStream out) throws IOException {
		byte[] magic = new byte[8];
		int count = in.readNBytes(magic, 0, 8);
		if (count != 8) {
			out.write(magic, 0, count);
			in.transferTo(out);
		} else if (Longs.fromByteArray(magic) == PngSurgeon.PNG_MAGIC) {
			try (var ps = new PngSurgeon(in, out)) {
				out.write(magic, 0, count);
				var baos = new ByteArrayOutputStream();
				byte[] buf = new byte[512];
				outer: while (true) {
					int type = ps.readChunkType();
					if (type == Chunk.tIME) {
						// useless chunk that destroys dedupe
						ps.skipChunkData();
					} else if (type == Chunk.tEXt) {
						int len = ps.getChunkLength();
						glass: if (len < 16384) {
							byte[] data;
							try {
								data = ps.readChunkData();
							} catch (CRCException e) {
								// uhh, okay. sure, you can enjoy that one
								ps.copyChunk();
								break glass;
							}
							var is = new ByteArrayInputStream(data);
							baos.reset();
							while (true) {
								String key = readNulString(is, buf, 80);
								if (key == null) {
									// corrupted tEXt chunk
									ps.writeChunk(Chunk.tEXt, data);
									continue outer;
								} else if (key.isEmpty()) {
									// EOS
									break;
								}
								boolean copy;
								switch (key) {
									case "date:timestamp":
									case "date:modify":
									case "date:create":
										// useless entries that destroy dedupe
										// (create is the closest to useful, but imagemagick will inject it in files that are missing a timestamp)
										copy = false;
										break;
									default:
										copy = true;
										break;
								}
								if (copy) {
									baos.write(key.getBytes(Charsets.ISO_8859_1));
									baos.write(0);
									transferNulBytes(is, buf, baos);
									baos.write(0);
								} else {
									skipNulBytes(is, buf);
								}
							}
							if (baos.size() != 0) {
								ps.writeChunk(Chunk.tEXt, baos);
							}
						} else {
							// alright have fun with that
							ps.copyChunk();
						}
					} else {
						ps.copyChunk();
						if (type == Chunk.IEND) break;
					}
				}
			}
		} else {
			out.write(magic, 0, count);
			in.transferTo(out);
		}
	}
	
	private static int readNulBytes(ByteArrayInputStream is, byte[] buf, int limit) {
		is.mark(limit);
		int count = is.readNBytes(buf, 0, limit);
		if (count == 0) return 0;
		int delimIdx = -1;
		for (int i = 0; i < count; i++) {
			if (buf[i] == 0) {
				delimIdx = i;
				break;
			}
		}
		is.reset();
		is.skip(delimIdx+1);
		return delimIdx;
	}
	
	private static void transferNulBytes(ByteArrayInputStream in, byte[] buf, OutputStream out) throws IOException {
		while (true) {
			int len = readNulBytes(in, buf, buf.length);
			if (len == 0) break;
			if (len == -1) {
				out.write(buf);
				in.skip(buf.length);
			} else {
				out.write(buf, 0, len);
				break;
			}
		}
	}
	
	private static void skipNulBytes(ByteArrayInputStream in, byte[] buf) throws IOException {
		while (true) {
			if (readNulBytes(in, buf, buf.length) != -1) break;
			in.skip(buf.length);
		}
	}
	
	private static String readNulString(ByteArrayInputStream is, byte[] buf, int limit) {
		int len = readNulBytes(is, buf, limit);
		if (len == 0) return "";
		if (len == -1) return null;
		return new String(buf, 0, len, Charsets.ISO_8859_1);
	}

}
