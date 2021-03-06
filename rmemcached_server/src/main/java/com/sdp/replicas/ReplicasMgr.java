package com.sdp.replicas;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Set;
import java.util.Vector;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.sdp.client.RMClient;
import com.sdp.common.EMSGID;
import com.sdp.example.Log;
import com.sdp.hotspot.HotspotIdentifier;
import com.sdp.messageBody.CtsMsg.nr_connected_mem_back;
import com.sdp.messageBody.CtsMsg.nr_read;
import com.sdp.messageBody.CtsMsg.nr_read_res;
import com.sdp.messageBody.CtsMsg.nr_register;
import com.sdp.messageBody.CtsMsg.nr_replicas_res;
import com.sdp.messageBody.CtsMsg.nr_write;
import com.sdp.messageBody.CtsMsg.nr_write_res;
import com.sdp.messageBody.StsMsg.nm_read;
import com.sdp.messageBody.StsMsg.nm_read_recovery;
import com.sdp.messageBody.StsMsg.nm_write_1;
import com.sdp.messageBody.StsMsg.nm_write_1_res;
import com.sdp.messageBody.StsMsg.nm_write_2;
import com.sdp.netty.NetMsg;
import com.sdp.server.MServer;
import com.sdp.server.ServerNode;
/**
 * 
 * @author martji
 *
 */

public class ReplicasMgr {
	HotspotIdentifier hotspotIdentifier;
	
	int serverId;
	Map<Integer, ServerNode> serversMap;
	MServer mServer;
	MemcachedClient mc;
	int protocol;
	
	private static int exptime = 60*60*24*10;
	ExecutorService pool = Executors.newCachedThreadPool();
	
	ConcurrentHashMap<Integer, RMClient> replicasClientMap = new ConcurrentHashMap<Integer, RMClient>();
	ConcurrentHashMap<Integer, MemcachedClient> spyClientMap = new ConcurrentHashMap<Integer, MemcachedClient>();
	public ConcurrentHashMap<String, Vector<Integer>> replicasIdMap = new ConcurrentHashMap<String, Vector<Integer>>();
	ConcurrentHashMap<String, LockKey> LockKeyMap = new ConcurrentHashMap<String, LockKey>();
	
	ConcurrentHashMap<Integer, Channel> clientChannelMap = new ConcurrentHashMap<Integer, Channel>();
	ConcurrentHashMap<String, Vector<Channel>> keyClientMap = new ConcurrentHashMap<String, Vector<Channel>>();
	ConcurrentHashMap<Integer, Map<String, Integer>> clientKeyMap = new ConcurrentHashMap<Integer, Map<String, Integer>>();
	
	public ReplicasMgr() {
		hotspotIdentifier = new HotspotIdentifier(this);
		new Thread(hotspotIdentifier).start();
	}
	
	public ReplicasMgr(int serverId, Map<Integer, ServerNode> serversMap, MServer mServer, int protocol) {
		this();
		this.serverId = serverId;
		this.serversMap = serversMap;
		this.mServer = mServer;
		this.protocol = protocol;
	}
	
	public void initThread() {
		new Thread(new Runnable() {
			public void run() {
				Iterator<Entry<Integer, ServerNode>> iterator = serversMap.entrySet().iterator();
				while (iterator.hasNext()) {
					Entry<Integer, ServerNode> map = iterator.next();
					int id = map.getKey();
					if (id != serverId) {
						ServerNode serverNode = map.getValue();
						RMClient rmClient = new RMClient(serverId, serverNode);
						replicasClientMap.put(id, rmClient);
						
						String host = serverNode.getHost();
						int memcachedPort = serverNode.getMemcached();
						try {
							MemcachedClient spyClient = new MemcachedClient(new InetSocketAddress(host, memcachedPort));
							spyClientMap.put(id, spyClient);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			}
		}).start();
	}
	
	public void setMemcachedClient(MemcachedClient mc) {
		this.mc = mc;
	}
	
	/**
	 * 
	 * @param msg : the request message
	 * Send the message to all replicas nodes.
	 */
	public void sendAllReplicas(String key, NetMsg msg) {
		if (replicasIdMap.containsKey(key)) {
			Vector<Integer> replicasList = replicasIdMap.get(key);
			int count = replicasList.size();
			for(int j = 0; j < count; j++) {
				int replicasId = replicasList.get(j);
				if (replicasId != serverId) {
					Channel channel = replicasClientMap.get(replicasId).getmChannel();
					channel.write(msg);
				}
			}
		}
	}
	
	public int getLockState(String key) {
		LockKey lock = LockKeyMap.get(key);
		if (lock == null) {
			return LockKey.unLock;
		}
		return lock.state;
	}

	public void setLockState(String key, Integer state) {
		LockKey lock = LockKeyMap.get(key);
		if (lock != null) {
			lock.state = state;
			LockKeyMap.put(key, lock);
		} else {
			Log.log.error("set Lock state error");
			return;
		}
	}
	
	/**
	 * 
	 * @return true if the pre-lock is null or the pre-lock is not badlock
	 */
	public boolean setLockKey(String key, LockKey lock) {
		LockKey lockKey = LockKeyMap.put(key, lock);	// the previous lock
		if (lockKey != null && lockKey.state != LockKey.badLock) {
			return true;
		}
		return lockKey == null;
	}

	public int desLockKeyCount(String key) {
		LockKey lock = LockKeyMap.get(key);
		if (lock != null) {
			lock.ncount--;
			LockKeyMap.put(key, lock);
			return lock.ncount;
		}
		return 0;
	}

	public boolean removeLock(String key) {
		return LockKeyMap.remove(key) != null;
	}
	
	public boolean getSetState(OperationFuture<Boolean> res) {
		try {
			if (res.get()) {
				return true;
			}
		} catch (Exception e) {}
		return false;
	}

	/**
	 * @param channel
	 * @param key
	 * @param failedId
	 * return the value and recovery the failed node
	 */
	private void handleReadFailed(Channel channel, String key, int failedId) {
		String oriKey = getOriKey(key);
		if (replicasIdMap.containsKey(oriKey)) {
			String value = null;
			if (failedId == serverId) {
				Vector<Integer> vector = replicasIdMap.get(oriKey);
				RMClient rmClient = replicasClientMap.get(vector.get(0));
				value = rmClient.readFromReplica(oriKey);
				if (value != null && value.length() > 0) {
					mc.set(oriKey, 3600, value);
				}
				
			} else {
				value = (String) mc.get(oriKey);
				if (value != null && value.length() > 0) {
					RMClient rmClient = replicasClientMap.get(failedId);
					rmClient.recoveryAReplica(oriKey, value);
				}
			}
			nr_read_res.Builder builder = nr_read_res.newBuilder();
			builder.setKey(key);
			builder.setValue(value);
			NetMsg msg = NetMsg.newMessage();
			msg.setMessageLite(builder);
			msg.setMsgID(EMSGID.nr_read_res);
			channel.write(msg);
		}
	}
	
	/**
	 * @param channel 
	 * @param key
	 * collect the register info
	 */
	private void handleRegister(Channel channel, String key) {
		hotspotIdentifier.handleRegister(key);
	}
	
	public void dealHotData() {
		if (!LocalSpots.hotspots.isEmpty()) {
			Set<String> hotspots = new HashSet<String>();
			hotspots.addAll(LocalSpots.hotspots);
			Map<String, Integer> hotitems = new HashMap<String, Integer>();
			String replicasInfo = mServer.getAReplica();
			List<Map.Entry<Integer, Double>> list = getReplicasInfoMap(replicasInfo);
			Log.log.info(Log.id + " new hotspot: " + hotspots);
			for (String key : hotspots) {
				int replicaId = getReplicaId(list, key);
				if (replicaId != -1) {
					boolean result = createReplica(key, replicaId);
					if (result) {
						Vector<Integer> vector = null;
						if (!replicasIdMap.containsKey(key)) {
							vector = new Vector<Integer>();
							vector.add(serverId);
							vector.add(replicaId);
							replicasIdMap.put(key, vector);
	
						} else {
							if (!replicasIdMap.get(key).contains(replicaId)) {
								replicasIdMap.get(key).add(replicaId);
								vector = replicasIdMap.get(key);
							}
						}
						hotitems.put(key, encodeReplicasInfo(vector));
					}
//					LocalSpots.removeHot(key);
				}
			}
			infoAllClient(hotitems);
			LocalSpots.hotspots = new HashSet<String>();
		}
	}
	
	public void dealColdData() {
		if (!LocalSpots.coldspots.isEmpty()) {
			Set<String> coldspots = new HashSet<String>();
			coldspots.addAll(LocalSpots.coldspots);
			Map<String, Integer> colditems = new HashMap<String, Integer>();
			Log.log.info(Log.id + " new coldspot: " + coldspots);
			for (String key : coldspots) {
				int replicaId = replicasIdMap.get(key).size()-1;
				replicasIdMap.get(key).remove(replicaId);
				colditems.put(key, encodeReplicasInfo(replicasIdMap.get(key)));
//				LocalSpots.removeCold(key);
				if (replicasIdMap.get(key).size() == 1) {
					replicasIdMap.remove(key);
				}
			}
			infoAllClient(colditems);
			LocalSpots.coldspots = new HashSet<String>();
		}
	}
	
	private List<Map.Entry<Integer, Double>> getReplicasInfoMap(String replicasInfo) {
		List<Map.Entry<Integer, Double>> list = null;
		if (replicasInfo == null || replicasInfo.length() == 0) {
			return list;
		}
		Gson gson = new GsonBuilder().enableComplexMapKeySerialization().create();
		Map<Integer, Double> cpuCostMap = gson.fromJson(replicasInfo, 
				new TypeToken<Map<Integer, Double>>() {}.getType());
		list = new ArrayList<Entry<Integer, Double>>(cpuCostMap.entrySet());
		Collections.sort(list, new Comparator<Entry<Integer, Double>>() {
			public int compare(Entry<Integer, Double> mapping1,
					Entry<Integer, Double> mapping2) {
				return mapping1.getValue().compareTo(mapping2.getValue());
			}
		});
		return list;
	}
	
	private int getReplicaId(List<Map.Entry<Integer, Double>> list, String key) {
		int replicaId = -1;
		HashSet<String> hosts = new HashSet<String>();
		HashSet<Integer> currentReplicas = new HashSet<Integer>();
		if (replicasIdMap.containsKey(key)) {
			currentReplicas = new HashSet<Integer>(replicasIdMap.get(key));
			for (int id : currentReplicas) {
				hosts.add(serversMap.get(id).getHost());
			}
		} else {
			currentReplicas.add(serverId);
			hosts.add(serversMap.get(serverId).getHost());
		}
		
		for (int i = 0; i < list.size(); i++) {
			int tmp = list.get(i).getKey();
			if (!currentReplicas.contains(tmp)) {
				if (!hosts.contains(serversMap.get(tmp).getHost())) {
					return tmp;
				} else if (replicaId == -1) {
					replicaId = tmp;
				}
			}
		}
		if (replicaId != -1 && replicaId != list.size() - 1) {
			Entry<Integer, Double> tmp = list.get(replicaId);
			list.set(replicaId, list.get(list.size() - 1));
			list.set(list.size() - 1, tmp);
		}
		return replicaId;
	}

	@SuppressWarnings("unused")
	private void infoAllClient(String key, Vector<Integer> vector) {
		Collection<Channel> clients = clientChannelMap.values();
		Vector<Integer> replicas = vector;
		Vector<Channel> tmp = new Vector<Channel>();
		tmp.addAll(clients);
		for (Channel mchannel: tmp) {
			if (!mchannel.isConnected()) {
				clients.remove(mchannel);
			}
		}
		if (replicas == null || replicas.size() == 0) {
			System.out.println("[ERROR] replication information lost.");
			return;
		}
		int replicaId = encodeReplicasInfo(replicas);
		nr_replicas_res.Builder builder = nr_replicas_res.newBuilder();
		builder.setKey(key);
		builder.setValue(Integer.toString(replicaId));
		NetMsg msg = NetMsg.newMessage();
		msg.setMessageLite(builder);
		msg.setMsgID(EMSGID.nr_replicas_res);
		for (Channel mchannel: clients) {
			mchannel.write(msg);
		}
	}
	
	private void infoAllClient(Map<String, Integer> colditems) {
		if (colditems == null || colditems.size() == 0) {
			return;
		}
		Collection<Channel> clients = clientChannelMap.values();
		Vector<Channel> tmp = new Vector<Channel>();
		tmp.addAll(clients);
		for (Channel mchannel: tmp) {
			if (!mchannel.isConnected()) {
				clients.remove(mchannel);
			}
		}
		
		Gson gson = new GsonBuilder().enableComplexMapKeySerialization().create();
		String replicasInfo = gson.toJson(colditems);
		nr_replicas_res.Builder builder = nr_replicas_res.newBuilder();
		builder.setKey("");
		builder.setValue(replicasInfo);
		NetMsg msg = NetMsg.newMessage();
		msg.setMessageLite(builder);
		msg.setMsgID(EMSGID.nr_replicas_res);
		for (Channel mchannel: clients) {
			mchannel.write(msg);
		}
	}

	public int encodeReplicasInfo(Vector<Integer> replicas) {
		int result = 0;
		for (int id : replicas) {
			result += Math.pow(2, id);
		}
		return result;
	}
	
	/**
	 * @param replicaId
	 */
	public boolean createReplica(String key, int replicaId) {
		RMClient replicaClient;
		if (replicasClientMap.containsKey(replicaId)) {
			replicaClient = replicasClientMap.get(replicaId);
		} else {
			replicaClient = new RMClient(serverId, serversMap.get(replicaId));
			if (replicaClient.getmChannel() == null) {
				return false;
			}
			replicasClientMap.put(replicaId, replicaClient);
		}
		
		String value = (String) mc.get(key);
		if (value == null || value.length() == 0) {
			System.out.println("[ERROR] no value fo this key: " + key);
			return false;
		}
		boolean out = replicaClient.recoveryAReplica(key, value);
		return out;
	}

	
	private String getOriKey(String key) {
		if (key.contains(":")) {
			return key.substring(key.indexOf(":") + 1);
		}
		return key;
	}

	public void handle(MessageEvent e) {
		NetMsg msg = (NetMsg) e.getMessage();
		switch (msg.getMsgID()) {
		case nr_connected_mem: {
			int clientId = msg.getNodeRoute();
			clientChannelMap.put(clientId, e.getChannel());
			System.out.println("[Netty] server hear channelConnected from client: " + e.getChannel());
			nr_connected_mem_back.Builder builder = nr_connected_mem_back.newBuilder();
			NetMsg send = NetMsg.newMessage();
			send.setMessageLite(builder);
			send.setMsgID(EMSGID.nr_connected_mem_back);
			e.getChannel().write(send);
		}
			break;
		case nm_connected: {
			System.out.println("[Netty] server hear channelConnected from other server: " + e.getChannel());
			final int id = msg.getNodeRoute();
			if (!replicasClientMap.containsKey(id)) {
				RMClient rmClient = new RMClient(serverId, serversMap.get(id));
				if (rmClient.getmChannel() != null) {
					replicasClientMap.put(id, rmClient);
				}
			} else if (replicasClientMap.get(id).getmChannel() == null || 
					!replicasClientMap.get(id).getmChannel().isConnected()) {
				new Thread(new Runnable() {
					public void run() {
						RMClient rmClient = replicasClientMap.get(id);
						rmClient.reconnect();
					}
				}).start();
			}
		}
			break;
		case nr_register: {
			nr_register msgLite = msg.getMessageLite();
			String key = msgLite.getKey(); // the original key
			handleRegister(e.getChannel(), key);
		}
			break;
		case nr_read: {
			nr_read msgLite = msg.getMessageLite();
			String key = msgLite.getKey();
			int failedId = msg.getNodeRoute();
			handleReadFailed(e.getChannel(), key, failedId);
		}
			break;
		case nm_read: {
			nm_read msgLite = msg.getMessageLite();
			String key = msgLite.getKey();
			String oriKey = getOriKey(key);
			String value = (String) mc.get(oriKey);
			if (value == null || value.length() == 0) {
				value = "";
			}
			
			nm_read.Builder builder = nm_read.newBuilder();
			builder.setKey(key);
			builder.setValue(value);
			NetMsg msg2 = NetMsg.newMessage();
			msg2.setMessageLite(builder);
			msg2.setMsgID(EMSGID.nm_read);
			e.getChannel().write(msg);
		}
			break;
		case nm_read_recovery: {
			nm_read_recovery msgLite = msg.getMessageLite();
			String key = msgLite.getKey();
			String oriKey = getOriKey(key);
			String value = msgLite.getValue();
			OperationFuture<Boolean> res = mc.set(oriKey, exptime, value);
			boolean setState = getSetState(res);
			if (!setState) {
				value = "";
			}
			
			nm_read_recovery.Builder builder = nm_read_recovery.newBuilder();
			builder.setKey(key);
			builder.setValue(value);
			NetMsg msg_back = NetMsg.newMessage();
			msg_back.setMessageLite(builder);
			msg_back.setMsgID(EMSGID.nm_read_recovery);
			e.getChannel().write(msg);
		}
			break;
		case nr_write: {
			handleWrite2(e, msg);
		}
			break;
		case nm_write_1: {
			nm_write_1 msgLite = msg.getMessageLite();
			String key = msgLite.getKey();
			String orikey = getOriKey(key);
			
			Integer state = getLockState(orikey);
			if (state != LockKey.unLock) {
				removeLock(orikey);
			}
			
			LockKey lockKey = new LockKey(serverId, 0, System.currentTimeMillis(), LockKey.waitLock);
			if (setLockKey(orikey, lockKey) == false) {
				Log.log.info("nm_write_1 Lock fail, please request again.");
				return;
			}
			
			nm_write_1_res.Builder builder = nm_write_1_res.newBuilder();
			builder.setKey(key);
			builder.setValue(msgLite.getValue());
			builder.setTime(msgLite.getTime());
			builder.setMemID(msgLite.getMemID());
			NetMsg send = NetMsg.newMessage();
			send.setNodeRoute(msg.getNodeRoute());
			send.setMessageLite(builder);
			send.setMsgID(EMSGID.nm_write_1_res);
			Channel channel = replicasClientMap.get(msgLite.getMemID()).getmChannel();
			channel.write(send);
		}
			break;
		case nm_write_1_res: {
			nm_write_1_res msgLite = msg.getMessageLite();
			String key = msgLite.getKey();
			String value = msgLite.getValue();
			String orikey = getOriKey(key);
			
			Integer clientId = msg.getNodeRoute();
			int thresod = clientKeyMap.get(clientId).get(orikey);
			if (desLockKeyCount(key) == thresod) {
				OperationFuture<Boolean> res = mc.set(orikey, exptime, value);
				boolean setState = getSetState(res);
				if (setState) {
					removeLock(msgLite.getKey());
					NetMsg response = getWriteResponse(key, value);
					clientChannelMap.get(msg.getNodeRoute()).write(response);
					
					nm_write_2.Builder builder = nm_write_2.newBuilder();
					builder.setKey(key);
					builder.setValue(value);
					builder.setMemID(msgLite.getMemID());
					builder.setTime(msgLite.getTime());
					NetMsg send = NetMsg.newMessage();
					send.setMessageLite(builder);
					send.setMsgID(EMSGID.nm_write_2);
					sendAllReplicas(orikey, send);
					
					removeLock(orikey);
				} else {
					setLockState(orikey, LockKey.badLock);
					Log.log.error("write to memcached server error");
					NetMsg response = getWriteResponse(key, "");
					clientChannelMap.get(msg.getNodeRoute()).write(response);
				}
			}
		}
			break;
		case nm_write_2: {
			nm_write_2 msgLite = msg.getMessageLite();
			String key = msgLite.getKey();
			String orikey = getOriKey(key);
			
			OperationFuture<Boolean> res = mc.set(orikey, exptime, msgLite.getValue());
			boolean setState = getSetState(res);
			if (setState) {
				removeLock(orikey);
			} else {
				setLockState(orikey, LockKey.badLock);
				Log.log.error("write in write_2 fail");
			}
		}
			break;
		default:
			break;
		}
	}

	public void handleWrite2(MessageEvent e, NetMsg msg) {
		//TODO
		final nr_write msgLite = msg.getMessageLite();
		String key = msgLite.getKey();
		String value = msgLite.getValue();
		
		String orikey = getOriKey(key);
//		Vector<Integer> replications = replicasIdMap.get(orikey);
//		int count = replications.size();
//		for (int i = 1; i < count; i++) {
//			MemcachedClient mClient = spyClientMap.get(replications.get(i));
//			MCThread thread = new MCThread(mClient, key, value);
//			pool.submit(thread);
//		}
		
		OperationFuture<Boolean> res = mc.set(orikey, exptime, value);
		boolean setState = getSetState(res);
		if (!setState) {
			value = "";
		}
		NetMsg response = getWriteResponse(key, value);
		e.getChannel().write(response);
	}
	
	public void handleWrite(MessageEvent e, NetMsg msg) {
		int clientlId = msg.getNodeRoute();
		nr_write msgLite = msg.getMessageLite();
		String key = msgLite.getKey();
		String orikey = getOriKey(key);
		String value = msgLite.getValue();
		
		Integer state = getLockState(orikey);
		if (state == LockKey.waitLock) {
			Log.log.info("write conflict, please request again.");
			NetMsg send = getWriteResponse(key, "");
			e.getChannel().write(send);
			return;
		}
		
		int count = replicasIdMap.get(orikey).size();
		LockKey lockKey = new LockKey(serverId, count, System.currentTimeMillis(), LockKey.waitLock);
		if (setLockKey(orikey, lockKey) == false) {
			Log.log.info("write lock conflict, please request again.");
			NetMsg send = getWriteResponse(key, "");
			e.getChannel().write(send);
			return;
		}
		
		if (!clientKeyMap.containsKey(clientlId)) {
			Map<String, Integer> keyMap = new HashMap<String, Integer>();
			clientKeyMap.put(clientlId, keyMap);
		}
		clientKeyMap.get(clientlId).put(orikey, getThreshold(count));
		nm_write_1.Builder builder = nm_write_1.newBuilder();
		builder.setKey(key);
		builder.setValue(value);
		builder.setMemID(serverId);		// set the master node of this set operation
		NetMsg send = NetMsg.newMessage();
		send.setNodeRoute(clientlId);	// set the requester of this set operation
		send.setMessageLite(builder);
		send.setMsgID(EMSGID.nm_write_1);
		sendAllReplicas(orikey, send);
	}

	private Integer getThreshold(int count) {
		if (protocol == 0) {
			return count - 1;
		}
		return count - count/protocol;
	}

	private NetMsg getWriteResponse(String key, String value) {
		nr_write_res.Builder builder = nr_write_res.newBuilder();
		builder.setKey(key);
		builder.setValue(value);
		NetMsg send = NetMsg.newMessage();
		send.setMessageLite(builder);
		send.setMsgID(EMSGID.nr_write_res);
		return send;
	}

	public void setMServer(MServer mServer) {
		this.mServer = mServer;
	}

}
