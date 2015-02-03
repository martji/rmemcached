package com.sdp.client;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.sdp.common.EMSGID;
import com.sdp.common.RegisterHandler;
import com.sdp.messageBody.requestMsg.nr_Read;
import com.sdp.messageBody.requestMsg.nr_write;
import com.sdp.netty.MDecoder;
import com.sdp.netty.MEncoder;
import com.sdp.netty.NetMsg;

/**
 * 
 * @author martji
 * 
 */

public class MClient implements RMemcachedClient{

	ClientBootstrap bootstrap;
	Channel mChannel = null;

	StringBuffer message = new StringBuffer();
	int clientNode = 0;
	MClientHandler mClientHandler;

	public static void main(String args[]) {
		RegisterHandler.initHandler();
		MClient mClient = new MClient(0, "192.168.3.201", 30000);

		long time = System.nanoTime();
		int opCount = 10000;
		for (int i = 0; i < opCount; i++) {
			System.out.println(i);
//			mClient.get();
			mClient.set();
		}
		System.out.println(">>" + (System.nanoTime() - time) / opCount / 1000f);
	}
	
	/**
	 * 
	 * @param host : rmemcached-server host
	 * @param port : rmemcached-client port
	 */
	public MClient(int clientNode, String host, int port) {
		this.clientNode = clientNode;
		init(clientNode, host, port);
	}

	public void init() {
		init(0, "127.0.0.1", 8080);
	}

	public void shutdown() {
		mChannel.close();
	}
	
	public void init(int clientNode, String host, int port) {
		try {
			bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
					Executors.newCachedThreadPool(),
					Executors.newCachedThreadPool()));

			mClientHandler = new MClientHandler(clientNode, message);
			bootstrap.setPipelineFactory(new MClientPipelineFactory(mClientHandler));

			ChannelFuture future = bootstrap.connect(
					new InetSocketAddress(host, port)).sync();
			while (!future.isDone()) {}
			mChannel = future.getChannel();
		} catch (Exception e) {
			e.printStackTrace();
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

	public String get(String key) {
		String value = "";
		String id = Long.toString(System.nanoTime());

		nr_Read.Builder builder = nr_Read.newBuilder();
		builder.setKey(key);
		builder.setTime(System.nanoTime());
		NetMsg msg = NetMsg.newMessage();
		msg.setNodeRoute(clientNode);
		msg.setMessageLite(builder);
		msg.setMsgID(EMSGID.nr_read);

		mClientHandler.requestList.put(id, msg);
		mClientHandler.queue.push(id);

		synchronized (id) {
			synchronized (mClientHandler.lock) {
				mClientHandler.lock.notify();
			}
			try {
				id.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		value = message.toString();
		return value;
	}

	public void get() {
		String key = Long.toString(System.nanoTime());
		System.out.println(">>request: " + key);
		System.out.println(">>response: " + get(key));
	}

	public boolean set(String key, String value) {
		String result = "";
		String id = Long.toString(System.nanoTime());
		
		nr_write.Builder builder = nr_write.newBuilder();
		builder.setKey(key);
		builder.setValue(value);
		builder.setTime(System.nanoTime());
		NetMsg msg = NetMsg.newMessage();
		msg.setNodeRoute(clientNode);
		msg.setMessageLite(builder);
		msg.setMsgID(EMSGID.nr_write);
		
		mClientHandler.requestList.put(id, msg);
		mClientHandler.queue.push(id);

		synchronized (id) {
			synchronized (mClientHandler.lock) {
				mClientHandler.lock.notify();
			}
			try {
				id.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		result = message.toString();
		return !result.isEmpty();
	}
	
	public void set() {
		String key = "testKey";
		String value = "This is a test.";
		System.out.println(">>request: " + key + ", " + value);
		System.out.println(">>response: " + set(key, value));
	}

	public boolean delete(String key) {
		// TODO Auto-generated method stub
		return false;
	}
}
