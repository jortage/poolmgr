package com.jortage.poolmgr;

import java.io.IOException;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.HandlerWrapper;
import org.eclipse.jetty.util.Jetty;

public class OuterHandler extends HandlerWrapper {

	public OuterHandler(Handler delegate) {
		setHandler(delegate);
	}
	
	@Override
	public void handle(String target, Request baseRequest, HttpServletRequest req, HttpServletResponse res) throws IOException, ServletException {
		res.setHeader("Server", "jortage-proxy");
		res.setHeader("Powered-By", "Jetty/"+Jetty.VERSION);
		res.setHeader("Clacks-Overhead", "GNU Natalie Nguyen, Shiina Mota");
		res.setHeader("Jeans-Teleshorted", Integer.toString((int)(Math.random()*200000)+70));
		if (target.isEmpty() || target.equals("/") || target.equals("/index.html")) {
			baseRequest.setHandled(true);
			res.setHeader("Location", "https://jortage.com");
			res.setStatus(301);
			return;
		}
		super.handle(target, baseRequest, req, res);
	}
	
}
