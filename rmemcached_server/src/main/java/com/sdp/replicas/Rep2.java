package com.sdp.replicas;

public class Rep2 {

}

/*
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
*/