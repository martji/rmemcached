package com.sdp.client;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.sdp.common.RegisterHandler;
import com.sdp.server.ServerNode;
/**
 * 
 * @author martji
 *
 */

public class MClientMain {

	Logger log;
	Map<Integer, ServerNode> serversMap;
	int replicasNum;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		MClientMain launch = new MClientMain();
		launch.start();
	}

	public void start() {
		PropertyConfigurator.configure(System.getProperty("user.dir") + "/config/log4j.properties");
		log = Logger.getLogger( MClientMain.class.getName());
		serversMap = new HashMap<Integer, ServerNode>();
		
		RegisterHandler.initHandler();
		getConfig();
		getServerList();
		
		MClientMgr mc = new MClientMgr(0, replicasNum);
		mc.init(serversMap);
		
		String key = "testKey";
		String value = "This is a test of an object blah blah es.";
		int runs = 10000;
		int start = 0;
		long begin = System.currentTimeMillis();
		for (int i = start; i < start+runs; i++) {
			System.out.println(i);
			System.out.println(">>request: " + key + ", " + value);
			System.out.println(">>response: " + mc.set(key + i, value));
		}
		long end = System.currentTimeMillis();
		long time = end - begin;
		System.out.println(runs + " sets: " + time + "ms");
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
				 int port = Integer.parseInt(server.elementText("port"));
				 String memcached = server.elementText("memcached");
				 
				 ServerNode serverNode = new ServerNode(id, host, port, memcached);
				 serversMap.put(id, serverNode);
	        }
		} catch (DocumentException e) {
			log.error("wrong serverlist.xml", e);
		}
	}
	
	public void getConfig() {
		String configPath = System.getProperty("user.dir") + "/config/config.properties";
		try {
			Properties properties = new Properties();
			properties.load(new FileInputStream(configPath));
			replicasNum = Integer.parseInt(properties.getProperty("replicasNum"));
		} catch (Exception e) {
			log.error("wrong config.properties", e);
		}
	}
}
