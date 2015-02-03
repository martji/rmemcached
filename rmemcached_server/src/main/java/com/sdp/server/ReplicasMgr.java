package com.sdp.server;

import java.util.Map;
import java.util.Random;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import com.sdp.client.MClient;
import com.sdp.netty.NetMsg;
/**
 * 
 * @author martji
 *
 */

public class ReplicasMgr {
	int num;
	int replicasNum;
	Map<Integer, ServerNode> serversMap;
	ConcurrentHashMap<Integer, Channel> clientChannelMap = new ConcurrentHashMap<Integer, Channel>();
	
	public void init(int num, int replicasNum, Map<Integer, ServerNode> serversMap) {
		this.num = num;
		this.replicasNum = replicasNum;
		this.serversMap = serversMap;
		
		for(int j =1; j < replicasNum; j++) {
			int replicasNode = (num + j) % serversMap.size();
			ServerNode serverNode = serversMap.get(replicasNode);
			String host = serverNode.getHost();
			int port = serverNode.getPort();
			try {
				MClient mClient = new MClient(num, host, port);
				while (mClient.getmChannel() == null) {
					Thread.sleep(30*1000);
					mClient.connect(host, port);
				}
				clientChannelMap.put(replicasNode, mClient.getmChannel());
			} catch (Exception e) {
				// TODO: handle exception
			}
		}
	}
	
	public void sendOneReplicas(NetMsg msg) {
		// random choose one replicas node for data
		Random r = new Random();
		int replicasNode = r.nextInt(replicasNum - 1) + 1;
		replicasNode = (num + replicasNode) % replicasNum;
		Channel channel = clientChannelMap.get(replicasNode);
		channel.write(msg);
	}
	
	public void sendAllReplicas(NetMsg msg) {
		// ask all nodes for data
		for(int j =1; j < replicasNum; j++) {
			int replicasNode = (num + j) % serversMap.size();
			Channel channel = clientChannelMap.get(replicasNode);
			channel.write(msg);
		}
	}
}
