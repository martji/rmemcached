package com.sdp.server;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import com.sdp.client.MClient;
import com.sdp.common.EMSGID;
import com.sdp.messageBody.memcachedmsg.nm_read;
import com.sdp.messageBody.memcachedmsg.nm_read_recovery;
import com.sdp.messageBody.memcachedmsg.nm_write_1;
import com.sdp.messageBody.memcachedmsg.nm_write_1_res;
import com.sdp.messageBody.memcachedmsg.nm_write_2;
import com.sdp.messageBody.requestMsg.nr_Connected_mem_back;
import com.sdp.messageBody.requestMsg.nr_Read;
import com.sdp.messageBody.requestMsg.nr_Read_res;
import com.sdp.messageBody.requestMsg.nr_write;
import com.sdp.messageBody.requestMsg.nr_write_res;
import com.sdp.netty.NetMsg;
/**
 * 
 * @author martji
 *
 */

public class ReplicasMgr {
	Logger logger;
	
	int id;
	int replicasNum;
	Map<Integer, ServerNode> serversMap;
	
	ConcurrentHashMap<Integer, Channel> clientChannelMap = new ConcurrentHashMap<Integer, Channel>();
	ConcurrentHashMap<String, LockKey> LockKeyMap = new ConcurrentHashMap<String, LockKey>();
	ConcurrentHashMap<Integer, Channel> webServerChannelMap = new ConcurrentHashMap<Integer, Channel>();
	ConcurrentHashMap<Integer, Map<String, Integer>> clientKeyMap = new ConcurrentHashMap<Integer, Map<String, Integer>>();
	
	public ReplicasMgr() {
		logger = Logger.getLogger(ReplicasMgr.class);
	}
	
	/**
	 * 
	 * @param id : the id of a replicasMgr
	 * @param replicasNum : the number of replicas, including its own
	 * @param serversMap : all serverNodes; 
	 * This method create links with other participant nodes, if the replicas node not start, 
	 * it will retry 30s later until make connection with all participants.
	 * All the channels of participants are put in clientChannelMap.
	 */
	public void init(int id, int replicasNum, Map<Integer, ServerNode> serversMap) {
		this.id = id;
		this.replicasNum = replicasNum;
		this.serversMap = serversMap;
		
		for(int j =1; j < replicasNum; j++) {
			int replicasId = (id + j) % serversMap.size();
			ServerNode serverNode = serversMap.get(replicasId);
			String host = serverNode.getHost();
			int port = serverNode.getPort();
			try {
				MClient mClient = new MClient(id, host, port);
				while (mClient.getmChannel() == null) {
					Thread.sleep(30*1000);
					mClient.connect(host, port);
				}
				clientChannelMap.put(replicasId, mClient.getmChannel());
			} catch (Exception e) {
			}
		}
	}
	
	/**
	 * 
	 * @param msg : the request message
	 * Send the message to one replicas node.
	 */
	public void sendOneReplicas(NetMsg msg) {
		// random choose one replicas node for data
		Random r = new Random();
		int replicasId = r.nextInt(replicasNum - 1) + 1;
		replicasId = (id + replicasId) % serversMap.size();
		Channel channel = clientChannelMap.get(replicasId);
		channel.write(msg);
	}
	
	/**
	 * 
	 * @param msg : the request message
	 * Send the message to all replicas nodes.
	 */
	public void sendAllReplicas(NetMsg msg) {
		// ask all nodes for data
		for(int j =1; j < replicasNum; j++) {
			int replicasNode = (id + j) % serversMap.size();
			Channel channel = clientChannelMap.get(replicasNode);
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
			System.err.println("set Lock state error");
			return;
		}
	}
	
	public boolean lockKey(String key, LockKey lock) {
		LockKey lockKey = LockKeyMap.put(key, lock);
		if (lockKey != null && (lockKey.state == 0 || lockKey.state == 1)) {
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
		} catch (Exception e) {
		} 
		return false;
	}
	
	public void handle2(MessageEvent e, MemcachedClient mc) {
		NetMsg msg = (NetMsg) e.getMessage();
		switch (msg.getMsgID()) {
		case nr_connected_mem: {
			int clientNode = msg.getNodeRoute();
			webServerChannelMap.put(clientNode, e.getChannel());
			
			nr_Connected_mem_back.Builder builder = nr_Connected_mem_back.newBuilder();
			NetMsg send = NetMsg.newMessage();
			send.setMessageLite(builder);
			send.setMsgID(EMSGID.nr_connected_mem_back);

			e.getChannel().write(send);
		}
			break;
		case nr_read: {
			nr_Read msgLite = msg.getMessageLite();
			String key = msgLite.getKey();
			String value = (String) mc.get(key);
			value = value == null ? "" : value;
			
			nr_Read_res.Builder builder = nr_Read_res.newBuilder();
			builder.setKey(key);
			builder.setValue(value);
			builder.setTime(msgLite.getTime());
			NetMsg send = NetMsg.newMessage();
			send.setMessageLite(builder);
			send.setMsgID(EMSGID.nr_read_res);

			e.getChannel().write(send);
		}
			break;
		case nr_write: {
			nr_write msgLite = msg.getMessageLite();
			String key = msgLite.getKey();
			String value = msgLite.getValue();
			
			OperationFuture<Boolean> res = mc.set(key, 3600, value);
			boolean setState = getSetState(res);
			if (!setState) {
				value = "";
				logger.error("write fail");
			}
			nr_write_res.Builder builder = nr_write_res.newBuilder();
			builder.setKey(key);
			builder.setValue(value);
			builder.setTime(msgLite.getTime());
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
	

	public void handle(MessageEvent e, MemcachedClient mc) {
		NetMsg msg = (NetMsg) e.getMessage();
		switch (msg.getMsgID()) {
		case nr_connected_mem: {
			int clientNode = msg.getNodeRoute();
			webServerChannelMap.put(clientNode, e.getChannel());
			
			nr_Connected_mem_back.Builder builder = nr_Connected_mem_back.newBuilder();
			NetMsg send = NetMsg.newMessage();
			send.setMessageLite(builder);
			send.setMsgID(EMSGID.nr_connected_mem_back);

			e.getChannel().write(send);
		}
			break;
		case nm_connected: {
			// connect from other server node
		}
			break;
		
		case nr_read: {
			nr_Read msgLite = msg.getMessageLite();
			String key = msgLite.getKey();
			String value = "";

			Integer state = getLockState(key);
			if (state != LockKey.badLock) {
				if (state == LockKey.unLock) {
					value = (String) mc.get(key);
				}
				nr_Read_res.Builder builder = nr_Read_res.newBuilder();
				builder.setKey(key);
				builder.setValue(value);
				builder.setTime(msgLite.getTime());
				NetMsg send = NetMsg.newMessage();
				send.setMessageLite(builder);
				send.setMsgID(EMSGID.nr_read_res);

				e.getChannel().write(send);
			} else {
				// ask other node for data
				nm_read.Builder builder = nm_read.newBuilder();
				builder.setKey(msgLite.getKey());
				builder.setTime(msgLite.getTime());
				NetMsg send = NetMsg.newMessage();
				send.setNodeRoute(msg.getNodeRoute());
				send.setMessageLite(builder);
				send.setMsgID(EMSGID.nm_read);
				
				sendOneReplicas(send);
			}
		}
			break;
		case nm_read: {
			// read request from other server node
			nm_read msgLite = msg.getMessageLite();
			String key = msgLite.getKey();
			String value = null;
			Integer state = getLockState(msgLite.getKey());
			if (state == LockKey.unLock) {
				value = (String) mc.get(key);
			}
			
			// return to webserver
			nr_Read_res.Builder builder = nr_Read_res.newBuilder();
			builder.setKey(key);
			builder.setTime(msgLite.getTime());
			builder.setValue(value);
			NetMsg send = NetMsg.newMessage();
			send.setMessageLite(builder);
			send.setMsgID(EMSGID.nr_read_res);
			webServerChannelMap.get(msg.getNodeRoute()).write(send);
			
			if (value != null) {
				// recover data
				nm_read_recovery.Builder builder1 = nm_read_recovery.newBuilder();
				builder1.setKey(key);
				builder1.setTime(msgLite.getTime());
				builder1.setValue(value);
				NetMsg send1 = NetMsg.newMessage();
				send1.setMessageLite(builder);
				send1.setMsgID(EMSGID.nm_read_recovery);
				e.getChannel().write(send1);
			}
		}
			break;
		case nm_read_recovery: {
			nm_read_recovery msgLite = msg.getMessageLite();
			Integer state = getLockState(msgLite.getKey());
			if (state == LockKey.waitLock) {
				logger.info("recovery fail because of waitlock.");
				return;
			} else if (state == LockKey.badLock) {
				removeLock(msgLite.getKey());
			}
			OperationFuture<Boolean> res = mc.set(msgLite.getKey(), 3600, msgLite.getValue());
			boolean setState = getSetState(res);
			if (!setState) {
				setLockState(msgLite.getKey(), LockKey.badLock);
				logger.error("read recovery fail");
			}
		}
			break;
		case nr_write: {
			nr_write msgLite = msg.getMessageLite();
			String key = msgLite.getKey();
			String value = msgLite.getValue();
			Integer state = getLockState(key);
			if (state == LockKey.waitLock) {
				logger.info("write conflict, please request again.");
				nr_write_res.Builder builder = nr_write_res.newBuilder();
				builder.setKey(key);
				builder.setValue("");
				builder.setTime(msgLite.getTime());
				NetMsg send = NetMsg.newMessage();
				send.setMessageLite(builder);
				send.setMsgID(EMSGID.nr_write_res);
				
				e.getChannel().write(send);
				return;
			}
			
			LockKey lockKey = new LockKey(id, replicasNum - 1, 
					System.currentTimeMillis(), LockKey.waitLock);
			if (lockKey(key, lockKey) == false) {
				logger.info("write lock conflict, please request again.");
				nr_write_res.Builder builder = nr_write_res.newBuilder();
				builder.setKey(key);
				builder.setValue("");
				builder.setTime(msgLite.getTime());
				NetMsg send = NetMsg.newMessage();
				send.setMessageLite(builder);
				send.setMsgID(EMSGID.nr_write_res);
				
				e.getChannel().write(send);
				return;
			}
			
			
			Integer clientlId = msg.getNodeRoute();
			if (!clientKeyMap.containsKey(clientlId)) {
				Map<String, Integer> keyMap = new HashMap<String, Integer>();
				clientKeyMap.put(clientlId, keyMap);
			}
			Map<String, Integer> keyMap = clientKeyMap.get(clientlId);
			keyMap.put(key, 0);
			
			if (replicasNum > 1) {
				nm_write_1.Builder builder = nm_write_1.newBuilder();
				builder.setKey(key);
				builder.setValue(value);
				builder.setMemID(id);
				builder.setTime(msgLite.getTime());
				NetMsg send = NetMsg.newMessage();
				send.setNodeRoute(clientlId);
				send.setMessageLite(builder);
				send.setMsgID(EMSGID.nm_write_1);
				
				sendAllReplicas(send);
			} else {
				OperationFuture<Boolean> res = mc.set(key, 3600, value);
				boolean setState = getSetState(res);
				if (setState) {
					removeLock(key);
					
				} else {
					setLockState(key, LockKey.badLock);
					value = "";
					logger.error("write fail");
				}
				nr_write_res.Builder builder = nr_write_res.newBuilder();
				builder.setKey(key);
				builder.setValue(value);
				builder.setTime(msgLite.getTime());
				NetMsg send = NetMsg.newMessage();
				send.setMessageLite(builder);
				send.setMsgID(EMSGID.nr_write_res);
				
				e.getChannel().write(send);
			}
		}
			break;
		case nm_write_1: {
			// write request from other server node, step 1
			nm_write_1 msgLite = msg.getMessageLite();
			Integer state = getLockState(msgLite.getKey());
			if (state == LockKey.waitLock) {
				removeLock(msgLite.getKey());
			} else if (state == LockKey.badLock) {
				removeLock(msgLite.getKey());
			}
			
			LockKey lockKey = new LockKey(id, 0,
					System.currentTimeMillis(), LockKey.waitLock);
			
			if (lockKey(msgLite.getKey(), lockKey) == false) {
				System.out.println("nm_write_1 Lock fail");
			}
			nm_write_1_res.Builder builder = nm_write_1_res.newBuilder();
			builder.setKey(msgLite.getKey());
			builder.setValue(msgLite.getValue());
			builder.setTime(msgLite.getTime());
			builder.setMemID(msgLite.getMemID());
			NetMsg send = NetMsg.newMessage();
			send.setNodeRoute(msg.getNodeRoute());
			send.setMessageLite(builder);
			send.setMsgID(EMSGID.nm_write_1_res);
			
			Channel channel = clientChannelMap.get(msgLite.getMemID());
			channel.write(send);
		}
			break;
		case nm_write_1_res: {
			// write request from other server node, step 1_res
			nm_write_1_res msgLite = msg.getMessageLite();
			String key = msgLite.getKey();
			Integer clientId = msg.getNodeRoute();
			int count = clientKeyMap.get(clientId).get(key) + 1;
			if (count < 0) {
				clientKeyMap.get(clientId).put(key, count);
				return;
			}
			
			if (desLockKeyCount(msgLite.getKey()) == 0) {
				OperationFuture<Boolean> res = mc.set(key, 3600, msgLite.getValue());
				boolean setState = getSetState(res);
				if (setState) {
					removeLock(msgLite.getKey());
					nr_write_res.Builder builder2 = nr_write_res.newBuilder();
					builder2.setKey(msgLite.getKey());
					builder2.setValue(msgLite.getValue());
					builder2.setTime(msgLite.getTime());
					NetMsg send2 = NetMsg.newMessage();
					send2.setMessageLite(builder2);
					send2.setMsgID(EMSGID.nr_write_res);
					webServerChannelMap.get(msg.getNodeRoute()).write(send2);
					
					nm_write_2.Builder builder = nm_write_2.newBuilder();
					builder.setKey(msgLite.getKey());
					builder.setValue(msgLite.getValue());
					builder.setMemID(msgLite.getMemID());
					builder.setTime(msgLite.getTime());
					NetMsg send = NetMsg.newMessage();
					send.setMessageLite(builder);
					send.setMsgID(EMSGID.nm_write_2);
					
					sendAllReplicas(send);
				} else {
					setLockState(msgLite.getKey(), LockKey.badLock);
					logger.error("write to memcached server error");
					
					nr_write_res.Builder builder2 = nr_write_res.newBuilder();
					builder2.setKey(msgLite.getKey());
					builder2.setValue("");
					builder2.setTime(msgLite.getTime());
					NetMsg send2 = NetMsg.newMessage();
					send2.setMessageLite(builder2);
					send2.setMsgID(EMSGID.nr_write_res);
					webServerChannelMap.get(msg.getNodeRoute()).write(send2);
				}
			}
		}
			break;
		case nm_write_2: {
			// write request from other server node, step 2
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
}
