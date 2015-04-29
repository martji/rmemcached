package com.sdp.server;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.sdp.client.RMClient;
import com.sdp.monitor.Monitor;
import com.sdp.netty.MDecoder;
import com.sdp.netty.MEncoder;

/**
 * 
 * @author martji
 * 
 */

public class MServer {
	MServerHandler mServerHandler;
	RMClient monitorClient;

	public MServer() {}
	
	/**
	 * 
	 * @param id : the id of the server instance
	 * @param monitorAddress : the address of the monitor node
	 * @param serversMap : all the server instances info
	 */
	public void init(int id, String monitorAddress, Map<Integer, ServerNode> serversMap, int protocol) {
		ServerNode serverNode = serversMap.get(id);
		String server = serverNode.getServer();
		mServerHandler = new MServerHandler(server, id, serversMap, protocol);
		int port = serverNode.getPort();
		initRServer(port);
		registerMonitor(id, monitorAddress, serversMap.get(id).getMemcached());
	}

	public void init(int id, String monitorAddress,
			Map<Integer, ServerNode> serversMap, MServerHandler mServerHandler) {
		this.mServerHandler = mServerHandler;
		ServerNode serverNode = serversMap.get(id);
		int port = serverNode.getPort();
		initRServer(port);
		mServerHandler.replicasMgr.initThread();
		registerMonitor(id, monitorAddress, serversMap.get(id).getMemcached());
		
	}
	
	/**
	 * 
	 * @param id : the id of the server instance
	 * @param monitorAddress : the address of the monitor node
	 * @param memcachedPort : the memcachedPort 
	 */
	private void registerMonitor(int id, String monitorAddress, int memcachedPort) {
		Monitor.getInstance().setPort(memcachedPort);
		String[] arr = monitorAddress.split(":");
		
		String host = arr[0];
		int port = Integer.parseInt(arr[1]);
		monitorClient = new RMClient(id, host, port);
		while (monitorClient.getmChannel() == null) {
			try {
				Thread.sleep(30*1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			monitorClient.connect(host, port);
		}
		Monitor.getInstance().setMonitorChannel(monitorClient.getmChannel());
	}

	public void initRServer(int port) {
		ServerBootstrap bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));

		bootstrap.setPipelineFactory(new MServerPipelineFactory(mServerHandler));
		bootstrap.setOption("child.tcpNoDelay", true);
		bootstrap.setOption("child.keepAlive", true);
		bootstrap.setOption("reuseAddress", true);
		bootstrap.bind(new InetSocketAddress(port));
		System.out.println("[Netty] server start.");
	}

	public int getAReplica() {
		return monitorClient.asynGetAReplica();
	}

	private class MServerPipelineFactory implements ChannelPipelineFactory {
		MServerHandler mServerHandler;
		
		public MServerPipelineFactory(MServerHandler mServerHandler) {
			this.mServerHandler = mServerHandler;
		}

		public ChannelPipeline getPipeline() throws Exception {
			ChannelPipeline pipeline = Channels.pipeline();
			pipeline.addLast("decoder", new MDecoder());
			pipeline.addLast("encoder", new MEncoder());
			pipeline.addLast("handler", mServerHandler);
			return pipeline;
		}
	}
	
}
