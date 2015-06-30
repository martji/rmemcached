package com.yahoo.ycsb.db;

import java.util.*;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.sdp.client.RMClient;
import com.sdp.common.RegisterHandler;
import com.sdp.server.ServerNode;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.ByteArrayByteIterator;

public class RMemcached extends DB {
	RMClient client;
	Properties props;
	Map<Integer, ServerNode> serversMap;

	public static final int OK = 0;
	public static final int ERROR = -1;
	public static final int NOT_FOUND = -2;
	
	private int setMode = 0;

	public void init() throws DBException {
		props = getProperties();
		String serversPath = props.getProperty("rmemcached.path");
		int recordCount = Integer.decode(props.getProperty("recordcount"));
		int mode = Integer.decode(props.getProperty("mode"));
		this.setMode = Integer.decode(props.getProperty("setmode", "0"));
		if (serversPath == null) {
			throw new DBException("rmemcached.path param must be specified");
		}
		serversMap = new HashMap<Integer, ServerNode>();
		RegisterHandler.initHandler();
		getServerList();
		

		try {
			int clientId = (int) System.nanoTime();
			System.out.println("clientId: " + clientId);
			client = new RMClient(clientId, mode, recordCount, serversMap);
		} catch (Exception e) {
			throw new DBException(e);
		}
	}
	
	public void cleanup() throws DBException {
		client.shutdown();
	}

	
	public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
		String values = client.get(table + ":" + key);
		if (values == null)
			return NOT_FOUND;
		if (values.length() == 0)
			return NOT_FOUND;

		result.put(key, new ByteArrayByteIterator(values.getBytes()));
		return OK;
	}

	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		return ERROR;
	}

	public int update(String table, String key, HashMap<String, ByteIterator> values) {
		HashMap<String, byte[]> new_values = new HashMap<String, byte[]>();
		for (String k : values.keySet()) {
			new_values.put(k, values.get(k).toArray());
		}

		Boolean f = false;
		switch (setMode) {
			case 0:
				f = client.set2R(table + ":" + key, new_values.toString());
				break;
			case 1:
				f = client.synchronousSet(table + ":" + key, new_values.toString());
				break;
			case 2:
				f = client.asynSet(table + ":" + key, new_values.toString());
				break;
			default:
				f = client.set(table + ":" + key, new_values.toString());
				break;
		}
		
		try {
			return f ? OK : ERROR;
		} catch (Exception e) {
			return ERROR;
		}
	}

	public int insert(String table, String key,
			HashMap<String, ByteIterator> values) {
		return update(table, key, values);
	}

	public int delete(String table, String key) {
//		client.delete(table + ":" + key);
		return OK; // FIXME check future
	}
	
	@SuppressWarnings({ "unchecked" })
	public void getServerList() {
		String serverListPath = System.getProperty("user.dir") + "/config/serverlist.xml";
		SAXReader sr = new SAXReader();
		try {
			Document doc = sr.read(serverListPath);
			Element root = doc.getRootElement();
			List<Element> childElements = root.elements();
	        for (Element server : childElements) {
				 int id = Integer.parseInt(server.elementText("id"));
				 String host = server.elementText("host");
				 int rport = Integer.parseInt(server.elementText("rport"));
				 int wport = Integer.parseInt(server.elementText("wport"));
				 int memcachedPort = Integer.parseInt(server.elementText("memcached"));
				 ServerNode serverNode = new ServerNode(id, host, rport, wport, memcachedPort);
				 serversMap.put(id, serverNode);
	        }
		} catch (DocumentException e) {
			e.printStackTrace();
		}
	}
}
