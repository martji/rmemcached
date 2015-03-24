package com.sdp.server;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.spy.memcached.MemcachedClient;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

/**
 * 
 * @author martji
 * 
 */

public class MServerHandler extends SimpleChannelUpstreamHandler {
	
	Logger logger;
	MemcachedClient mc;
	volatile MCPoolImpl mcPools;
	ReplicasMgr replicasMgr;
	ExecutorService handleThreadPools;
	
	public MServerHandler() {
		logger = Logger.getLogger(MServerHandler.class);
		handleThreadPools = Executors.newFixedThreadPool(50);
	}
	
	public MServerHandler(List<String> servers) {
		this();
		initLocalMc(servers);
	}
	
	public MServerHandler(List<String> servers, final int num, final int replicasNum,
			final Map<Integer, ServerNode> serversMap) {
		this();
		
		replicasMgr = new ReplicasMgr();
		new Thread(new Runnable() {
			public void run() {
				replicasMgr.init(num, replicasNum, serversMap);
			}
		}).start();
		
		initLocalMc(servers);
	}
	
	private void initLocalMc(List<String> servers) {
		List<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();
		for (String server : servers) {
			try {
				String host = server.split(":")[0];
				int port = Integer.parseInt(server.split(":")[1]);
				addrs.add(new InetSocketAddress(host, port));
			} catch (Exception e) {
				logger.error("wrong serverlist format: " + server, e);
			}
		}
		try {
//			mc = new MemcachedClient(addrs);
			mcPools = new MCPoolImpl(50, addrs);
		} catch (Exception e) {
			e.printStackTrace();
		}
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
		System.out.println("server hear channelConnected: " + e.getChannel());
	}
	
	private void handleMessage(MessageEvent e) {
		HandleThread handleThread = new HandleThread(mcPools, e, replicasMgr);
		handleThreadPools.execute(handleThread);
	}
}

class HandleThread implements Runnable {
	private MCPoolImpl mcPools;
	private MemcachedClient mc;
	private MessageEvent e;
	private ReplicasMgr replicasMgr;
	
	public HandleThread(MCPoolImpl mcPools, MessageEvent e, ReplicasMgr replicasMgr) {
		this.mcPools = mcPools;
		this.mc = mcPools.getMClient();
		this.e = e;
		this.replicasMgr = replicasMgr;
	}
	
	public void run() {
		replicasMgr.handle2(e, mc);
		mcPools.recoverMClient(mc);
	}
}

