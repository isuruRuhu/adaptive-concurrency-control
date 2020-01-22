package org.wso2.adaptive_concurrency_control;

import com.codahale.metrics.Timer;

import org.wso2.adaptive_concurrency_control.tomcat.StandardThreadExecutor;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;

public class NettyServer {

	int port;
	String test;
	StandardThreadExecutor executingPool;
	static Timer latencyTimer;



	public NettyServer(int portNum, String testName, StandardThreadExecutor pool, Timer latencyTimer) {
		this.port = portNum;
		this.test = testName;
		this.executingPool = pool;
		this.latencyTimer = latencyTimer;
	}

	public void start() throws Exception {

		EventLoopGroup bossGroup = new NioEventLoopGroup();
		EventLoopGroup workerGroup = new NioEventLoopGroup();

		try {
			ServerBootstrap b = new ServerBootstrap();
			b.childOption(ChannelOption.SO_RCVBUF, 2147483647); // Increase receive buffer size
			b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
					.childHandler(new ChannelInitializer<SocketChannel>() {

						@Override
						public void initChannel(SocketChannel ch) throws Exception {
							ChannelPipeline p = ch.pipeline();
							p.addLast(new HttpServerCodec());
							p.addLast("aggregator", new HttpObjectAggregator(1048576));
							p.addLast(new NettyServerHandler(test, executingPool));
						}
					}).option(ChannelOption.SO_BACKLOG, 1000000).childOption(ChannelOption.SO_KEEPALIVE, true);

			ChannelFuture f = b.bind(port).sync();

			f.channel().closeFuture().sync();
		} finally {
			workerGroup.shutdownGracefully();
			bossGroup.shutdownGracefully();
		}
	}

}
