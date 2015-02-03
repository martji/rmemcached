package com.sdp.server;

import java.util.ArrayList;
import java.util.List;

public class ServerNode {
	private int id;
	private String host;
	private int port;
	private List<String> memcached;
	
	public ServerNode(String host, int port, String memcached) {
		this.host = host;
		this.port = port;
		this.memcached = new ArrayList<String>();
		String[] memcachedList = memcached.split(",");
		for (String mcNode : memcachedList) {
			this.memcached.add(mcNode);
		}
	}
	
	public ServerNode(int id, String host, int port, String memcached) {
		this.id = id;
		this.host = host;
		this.port = port;
		this.memcached = new ArrayList<String>();
		String[] memcachedList = memcached.split(",");
		for (String mcNode : memcachedList) {
			this.memcached.add(mcNode);
		}
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
	public List<String> getMemcached() {
		return memcached;
	}
	public void setMemcached(List<String> memcached) {
		this.memcached = memcached;
	}
}
