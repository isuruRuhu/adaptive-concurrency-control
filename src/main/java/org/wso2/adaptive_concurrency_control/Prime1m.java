package org.wso2.adaptive_concurrency_control;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import com.codahale.metrics.Timer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpUtil;
import org.slf4j.LoggerFactory;

/**
 * Test to measure performance of Primality check
 */
public class Prime1m implements Runnable {

	private final AtomicInteger users;
	private FullHttpRequest msg;
	private ChannelHandlerContext ctx;
	private Timer.Context timerContext;

	public static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(AdaptiveConcurrencyControl.class);

	public Prime1m(ChannelHandlerContext ctx, FullHttpRequest msg, Timer.Context timerCtx, AtomicInteger users) {
		this.msg = msg;
		this.ctx = ctx;
		this.timerContext = timerCtx;
		this.users = users;
	}

	@Override
	public void run() {
		ByteBuf buf = null;
		try {
			Random rand = new Random();
//			int number = rand.nextInt((1000021) - 1000000 ) + 1000000;  //Generate random integer between 100000 and 100020
			int number = 1000003;  //Generate random integer between 100000 and 100020
			String resultString = "true";
			for (int i=2; i<number; i++) {
				if (number % i == 0) {
					resultString = "false";
					break;
				}
			}
			buf = Unpooled.copiedBuffer(resultString.getBytes());
		} catch (Exception e) {
			AdaptiveConcurrencyControl.LOGGER.error("Exception in Prime1m Run method", e);
		}
		
		boolean keepAlive = HttpUtil.isKeepAlive(msg);
		FullHttpResponse response = null;
		try {
			response = new DefaultFullHttpResponse(HTTP_1_1, OK, buf);
//			System.out.println(users.decrementAndGet());
			LOGGER.info(String.valueOf(users.decrementAndGet()));
		} catch (Exception e) {
			AdaptiveConcurrencyControl.LOGGER.error("Exception in Netty Handler", e);
		}
		String contentType = msg.headers().get(HttpHeaderNames.CONTENT_TYPE);
		if (contentType != null) {
			response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
		}
		response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
		if (!keepAlive) {
			ctx.write(response).addListener(ChannelFutureListener.CLOSE);
		} else {
			response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
			ctx.write(response);
		}
		ctx.flush();
		timerContext.stop(); // Stop Dropwizard metrics timer
	}
}