/*
Copyright (c) Microsoft Open Technologies, Inc.
All Rights Reserved
See License.txt in the project root for license information.
*/

package com.github.signalr4j.client.transport;

import com.github.signalr4j.client.*;
import com.github.signalr4j.client.http.HttpConnection;
import com.google.gson.Gson;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.drafts.Draft_6455;
import org.java_websocket.exceptions.InvalidDataException;
import org.java_websocket.framing.CloseFrame;
import org.java_websocket.framing.Framedata;
import org.java_websocket.handshake.ServerHandshake;
import org.java_websocket.util.Charsetfunctions;

import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implements the WebsocketTransport for the Java SignalR library Created by
 * stas on 07/07/14.
 */
public class WebsocketTransport extends HttpClientTransport {

	private static final String HTTP_SCHEME = "http";
	private static final String SECURE_HTTP_SCHEME = "https";

	private static final String WEBSCOCKET_SCHEME = "ws";
	private static final String SECURE_WEBSOCKET_SCHEME = "wss";

	private static final String HTTP_URL_START = HTTP_SCHEME + "://";
	private static final String SECURE_HTTP_URL_START = SECURE_HTTP_SCHEME + "://";

	private static final String WEBSOCKET_URL_START = WEBSCOCKET_SCHEME + "://";
	private static final String SECURE_WEBSOCKET_URL_START = SECURE_WEBSOCKET_SCHEME + "://";

	private String mPrefix;
	private static final Gson gson = new Gson();
	WebSocketClient mWebSocketClient;
	private UpdateableCancellableFuture<Void> mConnectionFuture;

	public WebsocketTransport(Logger logger) {
		super(logger);
	}

	public WebsocketTransport(Logger logger, HttpConnection httpConnection) {
		super(logger, httpConnection);
	}

	@Override
	public String getName() {
		return "webSockets";
	}

	@Override
	public boolean supportKeepAlive() {
		return true;
	}

	@Override
	public SignalRFuture<Void> start(final ConnectionBase connection, ConnectionType connectionType,
									 final DataResultCallback callback) {

		final String connectionUrl = connection.getUrl()
				.replace(HTTP_URL_START, WEBSOCKET_URL_START)
				.replace(SECURE_HTTP_URL_START, SECURE_WEBSOCKET_URL_START);

		final String connectionString = connectionType == ConnectionType.InitialConnection ? "connect" : "reconnect";

		Map<String, String> requestParams = new HashMap<>();
		requestParams.put("connectionToken", connection.getConnectionToken());
		requestParams.put("connectionData", connection.getConnectionData());
		requestParams.put("groupsToken", connection.getGroupsToken());
		requestParams.put("messageId", connection.getMessageId());
		requestParams.put("transport", getName());

		boolean isSsl = false;

//      Error on API < 24
//		String url = requestParams.keySet().stream()
//				.map(key -> key + "=" + encodeValue(requestParams.get(key)))
//				.collect(joining("&", connectionUrl + connectionString + "?", ""));

		ArrayList<String> keys = new ArrayList<>(requestParams.keySet());
		StringBuilder urlBuilder = new StringBuilder();
		urlBuilder.append(connectionUrl);
		urlBuilder.append(connectionString);
		urlBuilder.append("?");
		for (int i = 0; i < keys.size(); i++) {
			String key = keys.get(i);
			urlBuilder.append(key);
			urlBuilder.append("=");
			urlBuilder.append(encodeValue(requestParams.get(key)));
			if (i != keys.size() - 1) {
				urlBuilder.append("&");
			}
		}

		String url = urlBuilder.toString();

		if (connection.getQueryString() != null) {
			url += "&" + connection.getQueryString();
		}

		if (url.startsWith(SECURE_WEBSOCKET_SCHEME)) {
			isSsl = true;
		}

		mConnectionFuture = new UpdateableCancellableFuture<Void>(null);

		URI uri;
		try {
			uri = new URI(url);
		} catch (URISyntaxException e) {
			e.printStackTrace();
			mConnectionFuture.triggerError(e);
			return mConnectionFuture;
		}

		mWebSocketClient = new WebSocketClient(uri, new Draft_6455(), connection.getHeaders(), 0) {

			Exception e;

			@Override
			public void onOpen(ServerHandshake serverHandshake) {
				mConnectionFuture.setResult(null);
			}

			@Override
			public void onMessage(String s) {
				callback.onData(s);
			}

			@Override
			public void onClose(int i, String s, boolean b) {
				mWebSocketClient.close();
				if (i != CloseFrame.NORMAL || e != null) {
					connection.onError(e, true);
				}
			}

			@Override
			public void onError(Exception e) {
				mWebSocketClient.close();
				this.e = e;
			}

			@Override
			public void onFragment(Framedata frame) {
				try {
					String decodedString = Charsetfunctions.stringUtf8(frame.getPayloadData());

					if (decodedString.equals(",")) {
						return;
					}

					if (decodedString.equals("]}")) {
						return;
					}

					if (decodedString.endsWith(":[") || null == mPrefix) {
						mPrefix = decodedString;
						return;
					}

					String simpleConcatenate = mPrefix + decodedString;

					if (isJSONValid(simpleConcatenate)) {
						onMessage(simpleConcatenate);
					} else {
						String extendedConcatenate = simpleConcatenate + "]}";
						if (isJSONValid(extendedConcatenate)) {
							onMessage(extendedConcatenate);
						} else {
							log("invalid json received:" + decodedString, LogLevel.Critical);
						}
					}
				} catch (InvalidDataException e) {
					e.printStackTrace();
				}
			}
		};

    try {
      List<Proxy> proxies = ProxySelector.getDefault().select(new URI(connection.getUrl()));
      Proxy proxy = proxies.isEmpty() ? Proxy.NO_PROXY : proxies.get(0);
      mWebSocketClient.setProxy(proxy);
    } catch (URISyntaxException e) {
      log(e);
      e.printStackTrace();
    }

		mWebSocketClient.connect();

		connection.closed(new Runnable() {
			@Override
			public void run() {
				mWebSocketClient.close();
			}
		});

		return mConnectionFuture;
	}

	private String encodeValue(String value) {

		String result = "";

		try {
			if (value != null)
				result = URLEncoder.encode(value, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		return result;
	}

	private Socket createConnectedSocket(URI uri) throws IOException {
		String host = uri.getHost();
		boolean isSecureSocket = uri.toString().startsWith(SECURE_WEBSOCKET_URL_START);
		int port = isSecureSocket ? 443 : 80;

		boolean useProxy = Platform.useProxy();
		String proxyHost = Platform.getProxyHost();
		int proxyPort = Platform.getProxyPort();
		Proxy proxy = useProxy ? new Proxy(Proxy.Type.SOCKS, new InetSocketAddress(proxyHost, proxyPort))
				: Proxy.NO_PROXY;

		if (isSecureSocket) {
			SSLSocketFactory factory = (SSLSocketFactory) SSLSocketFactory.getDefault();
			if (useProxy) {
				Socket underlyingSocket = new Socket(proxy);
				underlyingSocket.connect(new InetSocketAddress(host, port), CONNECTION_TIMEOUT_MS);
				return factory.createSocket(underlyingSocket, proxyHost, proxyPort, true);
			} else {
				Socket socket = factory.createSocket();
				socket.connect(new InetSocketAddress(host, port), CONNECTION_TIMEOUT_MS);
				return socket;
			}
		} else {
			Socket socket = new Socket(proxy);
			socket.connect(new InetSocketAddress(host, port), CONNECTION_TIMEOUT_MS);
			return socket;
		}
	}

	@Override
	public SignalRFuture<Void> send(ConnectionBase connection, String data, DataResultCallback callback) {
		mWebSocketClient.send(data);
		return new UpdateableCancellableFuture<Void>(null);
	}

	private boolean isJSONValid(String test) {
		try {
			gson.fromJson(test, Object.class);
			return true;
		} catch (com.google.gson.JsonSyntaxException ex) {
			return false;
		}
	}
}
