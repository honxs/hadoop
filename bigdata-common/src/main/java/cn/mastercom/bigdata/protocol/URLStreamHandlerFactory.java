package cn.mastercom.bigdata.protocol;

import java.net.URLStreamHandler;

public class URLStreamHandlerFactory implements java.net.URLStreamHandlerFactory {
	public URLStreamHandler createURLStreamHandler(String protocol) {
		if (protocol.equals("me")) {
			return new cn.mastercom.bigdata.protocol.me.Handler();
		}
		return null;
	}
}
