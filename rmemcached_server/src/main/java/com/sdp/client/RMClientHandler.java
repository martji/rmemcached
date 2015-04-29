package com.sdp.client;

import java.util.HashMap;
import java.util.Map;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.sdp.common.EMSGID;
import com.sdp.messageBody.CtsMsg.nr_apply_replica_res;
import com.sdp.messageBody.CtsMsg.nr_cpuStats_res;
import com.sdp.messageBody.StsMsg.nm_connected;
import com.sdp.messageBody.StsMsg.nm_read_recovery;
import com.sdp.monitor.Monitor;
import com.sdp.netty.NetMsg;
import com.sdp.operation.BaseOperation;

/**
 * 
 * @author martji
 * 
 */

public class RMClientHandler extends SimpleChannelUpstreamHandler {

	public int id;
	Map<String, BaseOperation<?>> opMap;

	public RMClientHandler(int id) {
		this.id = id;
		opMap = new HashMap<String, BaseOperation<?>>();
	}

	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
		nm_connected.Builder builder = nm_connected.newBuilder();
		builder.setNum(this.id);
		NetMsg sendMsg = NetMsg.newMessage();
		sendMsg.setNodeRoute(id);
		sendMsg.setMsgID(EMSGID.nm_connected);
		sendMsg.setMessageLite(builder);
		e.getChannel().write(sendMsg);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
		if (e.getChannel().getLocalAddress() == null) {
			return;
		}
		e.getChannel().close();
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		handle(e);
	}

	private void handle(MessageEvent e) throws InterruptedException {
		NetMsg msg = (NetMsg) e.getMessage();
		switch (msg.getMsgID()) {
		case nm_connected_mem_back: {
			System.out.println("Connect to monitor successed.");
		}
			break;
		case nr_stats: {
			Double cpuCost = Monitor.getInstance().getCpuCost();
			
			nr_cpuStats_res.Builder builder = nr_cpuStats_res.newBuilder();
			builder.setValue(cpuCost.toString());
			NetMsg send = NetMsg.newMessage();
			send.setMessageLite(builder);
			send.setNodeRoute(id);
			send.setMsgID(EMSGID.nr_stats_res);
			
			e.getChannel().write(send);
		}
			break;
		case nr_apply_replica_res: {
			nr_apply_replica_res msgLite = msg.getMessageLite();
			String key = msgLite.getKey();
			String value = msgLite.getValue();
			handleStatsOp(key, value);
		}
			break;
		case nm_read_recovery: {
			nm_read_recovery msgLite = msg.getMessageLite();
			String key = msgLite.getKey();
			String value = msgLite.getValue();
			handleNmreadOp(key, value);
		}
			break;
		default:
			break;
		}
	}
	
	private void handleStatsOp(String key, String value) {
		if (opMap.containsKey(key)) {
			@SuppressWarnings("unchecked")
			BaseOperation<Integer> op = (BaseOperation<Integer>) opMap.get(key);
			if (value != null && value.length() > 0) {
				int replicaId = Integer.parseInt(value);
				op.getMcallback().gotdata(replicaId);
				opMap.remove(key);
			}
		}
		
	}
	
	private void handleNmreadOp(String key, String value) {
		if (opMap.containsKey(key)) {
			@SuppressWarnings("unchecked")
			BaseOperation<String> op = (BaseOperation<String>) opMap.get(key);
			op.getMcallback().gotdata(value);
			opMap.remove(key);
		}
		
	}

	public void addOpMap(String id, BaseOperation<?> op) {
		opMap.put(id, op);
	}
}
