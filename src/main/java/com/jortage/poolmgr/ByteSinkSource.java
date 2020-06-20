package com.jortage.poolmgr;

import java.io.Closeable;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;

public interface ByteSinkSource extends Closeable {
	ByteSink getSink();
	ByteSource getSource();
	@Override
	void close();
}
