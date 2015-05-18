package com.sdp.server;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import net.spy.memcached.MemcachedClient;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.sdp.replicas.ReplicasMgr;

/**
 * 
 * @author martji
 * 
 */

public class MServerHandler extends SimpleChannelUpstreamHandler {
	
	ReplicasMgr replicasMgr;
	MServer mServer = null;
	
	public MServerHandler() {}
	
	/**
	 * 
	 * @param server : the address of memcached server
	 * @param serverId : the id of the server instance
	 * @param serversMap : all server nodes
	 */
	public MServerHandler(String server, int serverId, Map<Integer, ServerNode> serversMap, int protocol) {
		
		this();
		replicasMgr = new ReplicasMgr(serverId, serversMap, mServer, protocol);
		
		String[] arr = server.split(":");
		String host = arr[0];
		int memcached = Integer.parseInt(arr[1]);
		List<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();
		addrs.add(new InetSocketAddress(host, memcached));
		MemcachedClient mc = null;
		try {
			mc  = new MemcachedClient(addrs);
		} catch (Exception e) {
			e.printStackTrace();
		}
		replicasMgr.setMemcachedClient(mc);
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
		handleMessage(e);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
		Channel channel = e.getChannel();
		channel.close();
	}

	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
	}
	
	private void handleMessage(MessageEvent e) {
		replicasMgr.handle(e);
	}

	public void setMServer(MServer mServer) {
		this.mServer = mServer;
		replicasMgr.setMServer(mServer);
	}
}

