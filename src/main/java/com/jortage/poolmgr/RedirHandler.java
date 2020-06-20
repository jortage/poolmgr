package com.jortage.poolmgr;

import java.io.IOException;
import java.util.List;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobAccess;
import com.google.common.base.Splitter;
import com.google.common.io.ByteStreams;

public final class RedirHandler extends AbstractHandler {
	private static final Splitter REDIR_SPLITTER = Splitter.on('/').limit(2).omitEmptyStrings();

	private final BlobStore dumpsStore;

	public RedirHandler(BlobStore dumpsStore) {
		this.dumpsStore = dumpsStore;
	}



	@Override
	public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		baseRequest.setHandled(true);
		List<String> split = REDIR_SPLITTER.splitToList(target);
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
					if (b.getMetadata().getContentMetadata().getContentLength() != null) {
						response.setHeader("Content-Length", b.getMetadata().getContentMetadata().getContentLength().toString());
					}
					response.setStatus(200);
					ByteStreams.copy(b.getPayload().openStream(), response.getOutputStream());
				} else {
					response.sendError(404);
				}
				return;
			}
			Poolmgr.reloadConfigIfChanged();
			try {
				boolean waited = false;
				while (true) {
					Object mutex = null;
					synchronized (Poolmgr.provisionalMaps) {
						mutex = Poolmgr.provisionalMaps.get(identity, name);
					}
					if (mutex == null) break;
					waited = true;
					synchronized (mutex) {
						try {
							mutex.wait();
						} catch (InterruptedException e) {}
					}
				}
				if (waited) {
					response.setHeader("Jortage-Waited", "true");
				}
				String hash = Queries.getMap(Poolmgr.dataSource, identity, name).toString();
				BlobAccess ba = Poolmgr.backingBlobStore.getBlobAccess(Poolmgr.bucket, Poolmgr.hashToPath(hash));
				if (ba != BlobAccess.PUBLIC_READ) {
					Poolmgr.backingBlobStore.setBlobAccess(Poolmgr.bucket, Poolmgr.hashToPath(hash), BlobAccess.PUBLIC_READ);
				}
				response.setHeader("Cache-Control", "public");
				response.setHeader("Location", Poolmgr.publicHost+"/"+Poolmgr.hashToPath(hash));
				response.setStatus(301);
			} catch (IllegalArgumentException e) {
				response.sendError(404);
			}
		}
	}
}