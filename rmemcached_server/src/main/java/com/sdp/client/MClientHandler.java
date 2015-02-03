package com.sdp.client;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.sdp.common.EMSGID;
import com.sdp.messageBody.memcachedmsg.nm_Connected;
import com.sdp.netty.NetMsg;

/**
 * 
 * @author martji
 * 
 */

public class MClientHandler extends SimpleChannelUpstreamHandler {

	public int id;

	public MClientHandler(int id) {
		this.id = id;
	}

	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
		System.out.println(e.getChannel());

		nm_Connected.Builder builder = nm_Connected.newBuilder();
		builder.setNum(this.id);
		NetMsg sendMsg = NetMsg.newMessage();
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
			System.out.println("--nm_connected_mem_back--");
		}
			break;
		default:
			break;
		}
	}
}
