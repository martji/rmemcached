package com.sdp.replicas;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import com.sdp.client.RMClient;
import com.sdp.common.EMSGID;
import com.sdp.hotspot.HotspotIdentifier;
import com.sdp.messageBody.CtsMsg.nr_connected_mem_back;
import com.sdp.messageBody.CtsMsg.nr_read;
import com.sdp.messageBody.CtsMsg.nr_read_res;
import com.sdp.messageBody.CtsMsg.nr_register;
import com.sdp.messageBody.CtsMsg.nr_write;
import com.sdp.messageBody.CtsMsg.nr_write_res;
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
	Logger logger;
	HotspotIdentifier hotspotIdentifier;
	
	int serverId;
	Map<Integer, ServerNode> serversMap;
	MServer mServer;
	MemcachedClient mc;
	int protocol;
	
	ConcurrentHashMap<Integer, RMClient> replicasClientMap = new ConcurrentHashMap<Integer, RMClient>();
	ConcurrentHashMap<String, Vector<Integer>> replicasIdMap = new ConcurrentHashMap<String, Vector<Integer>>();
	ConcurrentHashMap<String, LockKey> LockKeyMap = new ConcurrentHashMap<String, LockKey>();
	
	ConcurrentHashMap<Integer, Channel> clientChannelMap = new ConcurrentHashMap<Integer, Channel>();
	ConcurrentHashMap<Integer, Map<String, Integer>> clientKeyMap = new ConcurrentHashMap<Integer, Map<String, Integer>>();
	
	public ReplicasMgr() {
		logger = Logger.getLogger(ReplicasMgr.class);
		hotspotIdentifier = new HotspotIdentifier(System.currentTimeMillis());
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
		Vector<Integer> replicasList = replicasIdMap.get(key);
		int count = replicasList.size();
		for(int j =1; j < count; j++) {
			Channel channel = replicasClientMap.get(replicasList.get(j)).getmChannel();
			channel.write(msg);
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
			logger.error("set Lock state error");
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
	
	public void handle2(MessageEvent e) {
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
			int id = msg.getNodeRoute();
			System.out.println("[Netty] server hear channelConnected from other server: " + e.getChannel());
			if (!replicasClientMap.containsKey(id)) {
				RMClient rmClient = new RMClient(serverId, serversMap.get(id));
				if (rmClient.getmChannel() != null) {
					replicasClientMap.put(id, rmClient);
				}
			} else if (replicasClientMap.get(id).getmChannel() == null) {
				RMClient rmClient = replicasClientMap.get(id);
				rmClient.connect(serversMap.get(id));
			}
		}
			break;
		case nr_register: {
			nr_register msgLite = msg.getMessageLite();
			String key = msgLite.getKey();
			handleRegister(key);
		}
			break;
		case nr_read: {
			nr_read msgLite = msg.getMessageLite();
			String key = msgLite.getKey();
			int failedId = msg.getNodeRoute();
			handleReadFailed(e.getChannel(), key, failedId);
		}
			break;
		case nr_write: {
			nr_write msgLite = msg.getMessageLite();
			String key = msgLite.getKey();
			String value = msgLite.getValue();
			String oriKey = getOriKey(key);
			
			OperationFuture<Boolean> future = mc.set(oriKey, 3600, value);
			try {
				if (!future.get()) {
					value = "";
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			nr_write_res.Builder builder = nr_write_res.newBuilder();
			builder.setKey(key);
			builder.setValue(value);
			NetMsg send = NetMsg.newMessage();
			send.setMessageLite(builder);
			send.setMsgID(EMSGID.nr_write_res);
			e.getChannel().write(send);
		}
			break;
		default:
			break;
		}
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
	 * @param replicaId
	 */
	public boolean createReplica(String key, int replicaId) {
		String oriKey = getOriKey(key);
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
		
		String value = (String) mc.get(oriKey);
		boolean out = replicaClient.recoveryAReplica(oriKey, value);
		if (out) {
			if (replicasIdMap.containsKey(oriKey)) {
				replicasIdMap.get(oriKey).add(replicaId);
			} else {
				Vector<Integer> vector = new Vector<Integer>();
				vector.add(replicaId);
				replicasIdMap.put(oriKey, vector);
			}
		}
		return out;
	}

	/**
	 * @param key
	 * collect the register info
	 */
	private void handleRegister(String key) {
		// TODO
		if (LocalHotspots.contains(key)) {
			if (!replicasIdMap.containsKey(key)) {
				int replicaId = mServer.getAReplica();	// get a replica according to the current cpu cost
				createReplica(key, replicaId);			// create a replica for the key
			} else {
				Long timestamp = System.currentTimeMillis();
				hotspotIdentifier.handleRegister(timestamp, key);
			}
		}
	}
	
	private String getOriKey(String key) {
		if (key.contains(":")) {
			return key.substring(key.indexOf(":"));
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
			int id = msg.getNodeRoute();
			System.out.println("[Netty] server hear channelConnected from other server: " + e.getChannel());
			if (!replicasClientMap.containsKey(id)) {
				RMClient rmClient = new RMClient(serverId, serversMap.get(id));
				if (rmClient.getmChannel() != null) {
					replicasClientMap.put(id, rmClient);
				}
			} else if (replicasClientMap.get(id).getmChannel() == null) {
				RMClient rmClient = replicasClientMap.get(id);
				rmClient.connect(serversMap.get(id));
			}
		}
			break;
		case nr_register: {
			nr_register msgLite = msg.getMessageLite();
			String key = msgLite.getKey();
			handleRegister(key);
		}
			break;
		case nr_read: {
			nr_read msgLite = msg.getMessageLite();
			String key = msgLite.getKey();
			int failedId = msg.getNodeRoute();
			handleReadFailed(e.getChannel(), key, failedId);
		}
			break;
		case nm_read_recovery: {
			nm_read_recovery msgLite = msg.getMessageLite();
			String key = msgLite.getKey();
			String value = msgLite.getValue();
			OperationFuture<Boolean> res = mc.set(key, 3600, value);
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
			int clientlId = msg.getNodeRoute();
			nr_write msgLite = msg.getMessageLite();
			String key = msgLite.getKey();
			String value = msgLite.getValue();
			
			Integer state = getLockState(key);
			if (state == LockKey.waitLock) {
				logger.info("write conflict, please request again.");
				NetMsg send = getWriteResponse(key, "");
				e.getChannel().write(send);
				return;
			}
			
			int count = replicasIdMap.get(key).size();
			LockKey lockKey = new LockKey(serverId, count, System.currentTimeMillis(), LockKey.waitLock);
			if (setLockKey(key, lockKey) == false) {
				logger.info("write lock conflict, please request again.");
				NetMsg send = getWriteResponse(key, "");
				e.getChannel().write(send);
				return;
			}
			
			if (!clientKeyMap.containsKey(clientlId)) {
				Map<String, Integer> keyMap = new HashMap<String, Integer>();
				clientKeyMap.put(clientlId, keyMap);
			}
			clientKeyMap.get(clientlId).put(key, getThresod(count));
			nm_write_1.Builder builder = nm_write_1.newBuilder();
			builder.setKey(key);
			builder.setValue(value);
			builder.setMemID(serverId);		// set the master node of this set operation
			NetMsg send = NetMsg.newMessage();
			send.setNodeRoute(clientlId);	// set the requester of this set operation
			send.setMessageLite(builder);
			send.setMsgID(EMSGID.nm_write_1);
			sendAllReplicas(key, send);
		}
			break;
		case nm_write_1: {
			nm_write_1 msgLite = msg.getMessageLite();
			String key = msgLite.getKey();
			
			Integer state = getLockState(key);
			if (state != LockKey.unLock) {
				removeLock(key);
			}
			
			LockKey lockKey = new LockKey(serverId, 0, System.currentTimeMillis(), LockKey.waitLock);
			if (setLockKey(key, lockKey) == false) {
				logger.info("nm_write_1 Lock fail, please request again.");
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
			Integer clientId = msg.getNodeRoute();
			int thresod = clientKeyMap.get(clientId).get(key);
			if (desLockKeyCount(key) == thresod) {
				OperationFuture<Boolean> res = mc.set(key, 3600, msgLite.getValue());
				boolean setState = getSetState(res);
				if (setState) {
					removeLock(msgLite.getKey());
					NetMsg response = getWriteResponse(key, msgLite.getValue());
					clientChannelMap.get(msg.getNodeRoute()).write(response);
					
					nm_write_2.Builder builder = nm_write_2.newBuilder();
					builder.setKey(msgLite.getKey());
					builder.setValue(msgLite.getValue());
					builder.setMemID(msgLite.getMemID());
					builder.setTime(msgLite.getTime());
					NetMsg send = NetMsg.newMessage();
					send.setMessageLite(builder);
					send.setMsgID(EMSGID.nm_write_2);
					sendAllReplicas(msgLite.getKey(), send);
				} else {
					setLockState(msgLite.getKey(), LockKey.badLock);
					logger.error("write to memcached server error");
					NetMsg response = getWriteResponse(key, "");
					clientChannelMap.get(msg.getNodeRoute()).write(response);
				}
			}
		}
			break;
		case nm_write_2: {
			nm_write_2 msgLite = msg.getMessageLite();
			String key = msgLite.getKey();
			OperationFuture<Boolean> res = mc.set(key, 3600, msgLite.getValue());
			boolean setState = getSetState(res);
			if (setState) {
				removeLock(key);
			} else {
				setLockState(key, LockKey.badLock);
				logger.error("write in write_2 fail");
			}
		}
			break;
		default:
			break;
		}
	}

	private Integer getThresod(int count) {
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

}
