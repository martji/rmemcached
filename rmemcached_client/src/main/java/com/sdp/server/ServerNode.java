package com.sdp.server;

public class ServerNode {
	private int id;
	private String host;
	private int port;
	
	public ServerNode(String host, int port) {
		this.host = host;
		this.port = port;
	}
	
	public ServerNode(int id, String host, int port) {
		this.id = id;
		this.host = host;
		this.port = port;
	}

	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
}
