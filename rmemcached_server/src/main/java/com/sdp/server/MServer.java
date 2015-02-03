package com.sdp.server;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.sdp.common.RegisterHandler;
import com.sdp.netty.MDecoder;
import com.sdp.netty.MEncoder;

/**
 * 
 * @author martji
 * 
 */

public class MServer {
	ServerBootstrap bootstrap;
	MServerHandler mServerHandler = new MServerHandler();

	public static void main(String args[]) {
		RegisterHandler.initHandler();
		new MServer();
	}

	public MServer() {
		init(8080);
	}
	
	/**
	 * 
	 * @param port : rmemcached-server port
	 * @param servers : memcached servers
	 */
	public MServer(int port, List<String> servers) {
		mServerHandler = new MServerHandler(servers);
		init(port);
	}

	public MServer(ServerNode serverNode) {
		int port = serverNode.getPort();
		List<String> servers = serverNode.getMemcached();
		mServerHandler = new MServerHandler(servers);
		init(port);
	}
	
	public MServer(int num, int replicasNum, Map<Integer, ServerNode> serversMap) {
		ServerNode serverNode = serversMap.get(num);
		int port = serverNode.getPort();
		List<String> servers = serverNode.getMemcached();
		mServerHandler = new MServerHandler(servers, num, replicasNum, serversMap);
		init(port);
	}

	public void init(int port) {
		bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));

		bootstrap.setPipelineFactory(new MServerPipelineFactory(mServerHandler));
		bootstrap.setOption("child.tcpNoDelay", true);
		bootstrap.setOption("child.keepAlive", true);
		bootstrap.setOption("reuseAddress", true);
		bootstrap.bind(new InetSocketAddress(port));
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
