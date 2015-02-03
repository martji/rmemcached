package com.sdp.client;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.sdp.common.RegisterHandler;
import com.sdp.netty.MDecoder;
import com.sdp.netty.MEncoder;

/**
 * 
 * @author martji
 * 
 */

public class MClient{

	ClientBootstrap bootstrap;
	Channel mChannel = null;

	public Channel getmChannel() {
		return mChannel;
	}

	MClientHandler mClientHandler;

	public static void main(String args[]) {
		RegisterHandler.initHandler();
		new MClient(0, "192.168.3.201", 30000);
	}
	
	public MClient(int id, String host, int port) {
		init(id, host, port);
	}

	public void init() {
		init(0, "127.0.0.1", 8080);
	}

	public void shutdown() {
		mChannel.close();
	}
	
	public void init(int id, String host, int port) {
		try {
			bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
					Executors.newCachedThreadPool(),
					Executors.newCachedThreadPool()));

			mClientHandler = new MClientHandler(id);
			bootstrap.setPipelineFactory(new MClientPipelineFactory(mClientHandler));

			try {
				ChannelFuture future = bootstrap.connect(
					new InetSocketAddress(host, port)).sync();
				while (!future.isDone()) {}
				mChannel = future.getChannel();
			} catch (ChannelException e) {
				// server doesn't start
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	public void connect(String host, int port) {
		try {
			ChannelFuture future = bootstrap.connect(
				new InetSocketAddress(host, port)).sync();
			while (!future.isDone()) {}
			mChannel = future.getChannel();
		} catch (ChannelException e) {
			// server doesn't start
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	private class MClientPipelineFactory implements ChannelPipelineFactory {
		private MClientHandler mClientHandler;

		public MClientPipelineFactory(MClientHandler mClientHandler) {
			this.mClientHandler = mClientHandler;
		}

		public ChannelPipeline getPipeline() throws Exception {
			ChannelPipeline pipeline = Channels.pipeline();
			pipeline.addLast("decoder", new MDecoder());
			pipeline.addLast("encoder", new MEncoder());
			pipeline.addLast("handler", mClientHandler);
			return pipeline;
		}
	}
}
