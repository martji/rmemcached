package com.sdp.monitor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import com.sdp.common.EMSGID;
import com.sdp.messageBody.CtsMsg.nr_cpuStats;
import com.sdp.netty.NetMsg;

/**
 * 
 * @author martji
 *
 */
public class Monitor extends Thread {
	
	ConcurrentHashMap<Integer, Channel> serverChannelMap = new ConcurrentHashMap<Integer, Channel>();
	Map<Integer, Queue<Double>> cpuCostMap = new HashMap<Integer, Queue<Double>>();
	Map<Integer, Double> medianCpuCostMap = new HashMap<Integer, Double>();
	
	public Monitor() {
		
	}

	public void run() {
		while (true) {
			try {
				Thread.sleep(1000*5);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			Iterator<Entry<Integer, Channel>> servers = serverChannelMap.entrySet().iterator();
			while (servers.hasNext()) {
				Entry<Integer, Channel> server = servers.next();
				Channel channel = server.getValue();
				nr_cpuStats.Builder builder = nr_cpuStats.newBuilder();
				NetMsg send = NetMsg.newMessage();
				send.setMessageLite(builder);
				send.setMsgID(EMSGID.nr_stats);
				channel.write(send);
			}
		}
	}

	public void addServer(int clientNode, Channel channel) {
		serverChannelMap.put(clientNode, channel);
	}

	/**
	 * 
	 * @param clientNode
	 * @param cpuCost
	 */
	public void handle(int clientNode, String cpuCost) {
		Long currentTime = System.currentTimeMillis();
		System.out.println(currentTime + ": Instance: " + clientNode + " cpuCost: " + cpuCost);
		Double cost = Double.parseDouble(cpuCost);
		if (cpuCostMap.containsKey(clientNode)) {
			Queue<Double> arryCpuCost = cpuCostMap.get(clientNode);
			arryCpuCost.offer(cost);
			if (arryCpuCost.size() > 10) {
				arryCpuCost.poll();
			}
			medianCpuCostMap.put(clientNode, (medianCpuCostMap.get(clientNode) + cost) / 2);
		} else {
			Queue<Double> arryCpuCost = new LinkedList<Double>();
			arryCpuCost.offer(cost);
			cpuCostMap.put(clientNode, arryCpuCost);
			medianCpuCostMap.put(clientNode, cost);
		}
	}

	/**
	 * 
	 * @param clientNode
	 */
	public int chooseReplica(int clientNode) {
		int replicaNum = clientNode;
		Iterator<Entry<Integer, Double>> medianCosts = medianCpuCostMap.entrySet().iterator();
		while (medianCosts.hasNext()) {
			Entry<Integer, Double> costMap = medianCosts.next();
			if (costMap.getKey() != clientNode) {
				if (costMap.getValue() < medianCpuCostMap.get(replicaNum)) {
					replicaNum = costMap.getKey();
				}
			}
		}
		return replicaNum;
	}

}
