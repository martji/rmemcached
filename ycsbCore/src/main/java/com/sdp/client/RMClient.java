package com.sdp.client;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.jboss.netty.util.internal.ConcurrentHashMap;

import com.sdp.server.ServerNode;
/**
 * 
 * @author martji
 *
 */

public class RMClient {

	int clientId;
	int mode = 0;
	int recordCount = 1;
	Map<Integer, RMemcachedClientImpl> clientMap;
	Vector<Integer> clientIdVector;
	/**
	 * keyReplicaMap : save the replicated key and the replica
	 */
	ConcurrentMap<String, Vector<Integer>> keyReplicaMap;
	ExecutorService pool = Executors.newCachedThreadPool();
	
	/**
	 * 
	 * @param clientId
	 * @param serversMap
	 */
	public RMClient(int clientId, Map<Integer, ServerNode> serversMap) {
		this(clientId);
		init(serversMap);
	}
	
	public RMClient(int clientId, int mode, int recordCount, Map<Integer, ServerNode> serversMap) {
		this(clientId);
		this.mode = mode;
		this.recordCount = recordCount;
		init(serversMap);
	}
	
	public RMClient(int clientId) {
		this.clientId = clientId;
		clientMap = new HashMap<Integer, RMemcachedClientImpl>();
		keyReplicaMap = new ConcurrentHashMap<String, Vector<Integer>>();
		clientIdVector = new Vector<Integer>();
	}

	public void init(Map<Integer, ServerNode> serversMap) {
		Collection<ServerNode> serverList = serversMap.values();
		for (ServerNode serverNode : serverList) {
			int serverId = serverNode.getId();
			RMemcachedClientImpl rmClient = new RMemcachedClientImpl(clientId, serverNode, keyReplicaMap);
			clientMap.put(serverId, rmClient);
			clientIdVector.add(serverId);
		}
		Collections.sort(clientIdVector);
	}
	
	public void shutdown() {
		Collection<RMemcachedClientImpl> clientList = clientMap.values();
		for (RMemcachedClientImpl mClient : clientList) {
			mClient.shutdown();
		}
		pool.shutdown();
	}
	
	/**
	 * 
	 * @param key
	 * @return get the hash value of the key and map it to the serverId
	 */
	public int gethashMem(String key) {
		int clientsNum = clientIdVector.size();
		if (clientsNum == 1) {
			return 0;
		}
		int leaderIndex = key.hashCode() % clientsNum;
		leaderIndex = Math.abs(leaderIndex);
		return clientIdVector.get(leaderIndex);
	}
	
	public int getOneReplica(String key) {
		Vector<Integer> vector = keyReplicaMap.get(key);
		int index = new Random().nextInt(vector.size());
		return vector.get(index);
	}
	
	public int getSliceMem(String key) {
		int index = key.lastIndexOf("user") + 4;
		int keyNum = Integer.decode(key.substring(index));
		
		int clientsNum = clientIdVector.size();
		if (clientsNum == 1) {
			return 0;
		}
		int gap = this.recordCount / clientsNum;
		int leaderIndex = keyNum / gap;
		leaderIndex = Math.abs(leaderIndex);
		return clientIdVector.get(leaderIndex);
	}
	
	public String get(String key) {
		String value = null;
		RMemcachedClientImpl rmClient;
		int masterId;
		if (mode == 0) {
			masterId = gethashMem(key);
		} else {
			masterId = getSliceMem(key);
		}
		
		if (keyReplicaMap.containsKey(key)) {
			int replicasId = getOneReplica(key);
			rmClient = clientMap.get(replicasId);
			value = rmClient.get(key, masterId == replicasId);
			if (value == null | value.length() == 0) {
				rmClient = clientMap.get(masterId);
				value = rmClient.asynGet(key, replicasId);
			}
		} else {
			rmClient = clientMap.get(masterId);
			value = rmClient.get(key, true);
		}
		return value;
	}
	
	public boolean set(String key, String value) {
		boolean result = false;
		int masterId;
		if (mode == 0) {
			masterId = gethashMem(key);
		} else {
			masterId = getSliceMem(key);
		}
		RMemcachedClientImpl rmClient = clientMap.get(masterId);
		result = rmClient.set(key, value);
		return result;
	}
	
	public boolean set2R(String key, String value) {
		boolean result = false;
		int masterId;
		if (mode == 0) {
			masterId = gethashMem(key);
		} else {
			masterId = getSliceMem(key);
		}
		RMemcachedClientImpl rmClient = clientMap.get(masterId);
		result = rmClient.asynSet2R(key, value);
		return result;
	}
	
	public boolean synchronousSet(String key, String value) {
		boolean result = false;
		int masterId;
		if (mode == 0) {
			masterId = gethashMem(key);
		} else {
			masterId = getSliceMem(key);
		}
		if (!keyReplicaMap.containsKey(key)) {
			RMemcachedClientImpl rmClient = clientMap.get(masterId);
			result = rmClient.synchronousSet(key, value);
		} else {
			Vector<Integer> replications = keyReplicaMap.get(key);
			for (int replicasId : replications) {
				if (replicasId != masterId) {
					RMemcachedClientImpl rmClient = clientMap.get(replicasId);
					MCThread thread = new MCThread(rmClient, key, value);
					pool.submit(thread);
				}
			}
			RMemcachedClientImpl rmClient = clientMap.get(masterId);
			result = rmClient.synchronousSet(key, value);
		}
		return result;
	}
	
	public boolean asynSet(String key, String value) {
		boolean result = false;
		int masterId;
		if (mode == 0) {
			masterId = gethashMem(key);
		} else {
			masterId = getSliceMem(key);
		}
		if (!keyReplicaMap.containsKey(key)) {
			RMemcachedClientImpl rmClient = clientMap.get(masterId);
			result = rmClient.synchronousSet(key, value);
		} else {
			Vector<Integer> replications = keyReplicaMap.get(key);
			for (int replicasId : replications) {
				if (replicasId != masterId) {
					RMemcachedClientImpl rmClient = clientMap.get(replicasId);
					MCThread thread = new MCThread(rmClient, key, value);
					Future<Boolean> f = pool.submit(thread);
					try {
						if (!f.get()) {
							return false;
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
			RMemcachedClientImpl rmClient = clientMap.get(masterId);
			result = rmClient.synchronousSet(key, value);
		}
		return result;
	}
}


