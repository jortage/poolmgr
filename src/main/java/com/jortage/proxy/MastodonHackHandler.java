package com.jortage.proxy;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
		if (ua != null && ua.contains("Mastodon") && !ua.contains("Pleroma")
				&& request.getHeader("Jortage-Dont202") == null && request.getMethod().equals("POST")) {
			// Mastodon's uploader has an extremely short timeout.
			// Wait a short while, and if the response still hasn't been committed, send a 202 to avoid the timeout.
			shortCircuit = sched.schedule(() -> {
				try {
					while (!request.getInputStream().isFinished()) {
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
						}
					}
					response.setStatus(202);
				} catch (IOException e) {
				}
			}, 1000, TimeUnit.MILLISECONDS);
		}
		super.handle(target, baseRequest, request, response);
		if (shortCircuit != null) shortCircuit.cancel(false);
	}

}
