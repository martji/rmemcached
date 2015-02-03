package com.sdp.client;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.sdp.server.ServerNode;
/**
 * 
 * @author martji
 *
 */

public class MClientMgr {

	int clientId;
	int replicasNum;
	Map<Integer, MClient> clientMap = new HashMap<Integer, MClient>();
	
	/**
	 * 
	 * @param clientId
	 * @param replicasNum
	 * @param serversMap
	 */
	public MClientMgr(int clientId, int replicasNum, Map<Integer, ServerNode> serversMap) {
		this.clientId = clientId;
		this.replicasNum = replicasNum;
		init(serversMap);
	}
	
	public MClientMgr(int clientId, int replicasNum) {
		this.clientId = clientId;
		this.replicasNum = replicasNum;
	}
	
	public void init(Map<Integer, ServerNode> serversMap) {
		Collection<ServerNode> serverList = serversMap.values();
		for (ServerNode serverNode : serverList) {
			int serverId = serverNode.getId();
			String host = serverNode.getHost();
			int port = serverNode.getPort();
			
			MClient mClient = new MClient(clientId, host, port);
			clientMap.put(serverId, mClient);
		}
	}
	
	public MClient randMClient(Integer hash) {
		Random random = new Random();
		int clientsNum = clientMap.size();
		int index = random.nextInt(replicasNum);
		for (int i = 0; i < replicasNum; i++) {
			int num = (hash + i + index + clientsNum) % clientsNum;
			MClient mClient = clientMap.get(num);
			if (mClient != null) {
				return mClient;
			}
		}
		return null;
	}
	
	public MClient leaderClient(Integer hash) {
		int clientsNum = clientMap.size();
		for (int i = 0; i < replicasNum; i++) {
			int num = (hash + i + clientsNum)	% clientsNum;
			MClient mClient = clientMap.get(num);
			if (mClient != null) {
				return mClient;
			}
		}
		return null;
	}
	
	public int gethashMem(String key) {
		return Math.abs(key.hashCode() % clientMap.size());
	}
	
	public String get(String key) {
		String value = "";
		MClient mClient = randMClient(key.hashCode());
		value = mClient.get(key);
		return value;
	}
	
	public boolean set(String key, String value) {
		boolean result = false;
		MClient mClient = leaderClient(key.hashCode());
		result = mClient.set(key, value);
		return result;
	}
}
