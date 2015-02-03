package com.sdp.server;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.sdp.common.RegisterHandler;

/**
 * 
 * @author martji
 *
 */
public class MServerMain {

	final int TWOPHASECOMMIT = 0;
	final int PAXOS = 1;
	final int WEAK = 2;
	
	Logger log;
	Map<Integer, ServerNode> serversMap;
	int replicasNum;
	int protocol;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		MServerMain lanuch = new MServerMain();
		lanuch.start();
	}

	public void start() {
		PropertyConfigurator.configure(System.getProperty("user.dir") + "/config/log4j.properties");
		log = Logger.getLogger( MServerMain.class.getName());
		serversMap = new HashMap<Integer, ServerNode>();
		
		RegisterHandler.initHandler();
		getConfig();
		getServerList();
		
		int num = getMemcachedNumber();
		new MServer(num, replicasNum, serversMap);
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
			String protocolName = properties.getProperty("consistencyProtocol").toString();
			if(protocolName.equals("twoPhaseCommit")){
				protocol = TWOPHASECOMMIT;
			} else if(protocolName.equals("paxos")){
				protocol = PAXOS;
			}else if(protocolName.equals("weak")){
				protocol = WEAK;
			}else{
				log.error("consistency protocol input error");
			}
		} catch (Exception e) {
			log.error("wrong config.properties", e);
		}
	}
	
	@SuppressWarnings("resource")
	public int getMemcachedNumber() {
		System.out.print("Please input the server number:");
		Scanner scanner = new Scanner(System.in);
		return Integer.decode(scanner.next());
	}
}
