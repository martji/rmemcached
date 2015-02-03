package com.sdp.client;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.sdp.common.EMSGID;
import com.sdp.messageBody.requestMsg.nr_Connected_mem;
import com.sdp.messageBody.requestMsg.nr_Read_res;
import com.sdp.netty.NetMsg;

/**
 * 
 * @author martji
 * 
 */

public class MClientHandler extends SimpleChannelUpstreamHandler {

	public Stack<String> queue;
	public StringBuffer message;
	public int clientNode;
	public Object lock = new Object();
	public Map<String, NetMsg> requestList;

	public MClientHandler(int clientNode, StringBuffer message) {
		this.clientNode = clientNode;
		this.message = message;
		this.queue = new Stack<String>();
		this.requestList = new HashMap<String, NetMsg>();
	}

	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
		System.out.println(e.getChannel());

		nr_Connected_mem.Builder builder = nr_Connected_mem.newBuilder();
		NetMsg send = NetMsg.newMessage();
		send.setNodeRoute(clientNode);
		send.setMsgID(EMSGID.nr_connected_mem);
		send.setMessageLite(builder);
		e.getChannel().write(send);
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
		case nr_connected_mem_back: {
			System.out.println("--nr_connected_mem_back--");
		}
			break;
		case nr_read_res: {
			nr_Read_res msgBody = msg.getMessageLite();
			String value = msgBody.getValue();
			message.setLength(0);
			message.append(value);
			String obj = queue.pop();
			synchronized (obj) {
				obj.notify();
			}
		}
			break;
		default:
			break;
		}

		while (queue.isEmpty()) {
			synchronized (lock) {
				if (queue.isEmpty()) {
					lock.wait();
				}
			}
		}
		String id = queue.peek();
		NetMsg request = requestList.get(id);
		e.getChannel().write(request);
	}
}
