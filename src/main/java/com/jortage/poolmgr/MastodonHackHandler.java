package com.jortage.poolmgr;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.HandlerWrapper;

public class MastodonHackHandler extends HandlerWrapper {

	private static final ScheduledExecutorService sched = Executors.newScheduledThreadPool(2);
	
	public MastodonHackHandler(Handler delegate) {
		setHandler(delegate);
	}
	
	@Override
	public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		String ua = request.getHeader("User-Agent");
		ScheduledFuture<?> shortCircuit = null;
		if (!Poolmgr.readOnly && ua != null && ua.contains("aws-sdk-ruby") && request.getHeader("Jortage-Dont202") == null
				&& request.getQueryString() == null && request.getHeader("x-amz-copy-source") == null
				&& (request.getMethod().equals("POST") || request.getMethod().equals("PUT"))) {
			// Mastodon's uploader has a very short timeout.
			// Wait a short while, and if the response still hasn't been committed, send a 202 to avoid the timeout.
			shortCircuit = sched.schedule(() -> {
				try {
					while (!request.getInputStream().isFinished()) {
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
						}
					}
					response.sendError(202);
				} catch (IOException e) {
				}
			}, 2000, TimeUnit.MILLISECONDS);
		}
		try {
			super.handle(target, baseRequest, request, response);
		} finally {
			if (shortCircuit != null) shortCircuit.cancel(false);
		}
	}

}
