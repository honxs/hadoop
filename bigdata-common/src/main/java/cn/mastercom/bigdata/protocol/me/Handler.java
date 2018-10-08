package cn.mastercom.bigdata.protocol.me;

import java.io.IOException;
import java.net.Proxy;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

public class Handler extends URLStreamHandler {
	@Override
	protected URLConnection openConnection(URL url) throws IOException {
		return new MEURLConnection(url);
	}
	
	@Override
	protected URLConnection openConnection(URL url, Proxy proxy) throws IOException {
		return new MEURLConnection(url, proxy);
	}
}
