package org.wso2.adaptive_concurrency_control;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;

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

/**
 * Test to measure performance of Database Write
 */
public class DbWrite implements Runnable {

	private FullHttpRequest msg;
	private ChannelHandlerContext ctx;
	private Timer.Context timerContext;

	public DbWrite(ChannelHandlerContext ctx, FullHttpRequest msg, Timer.Context timerCtx) {
		this.msg = msg;
		this.ctx = ctx;
		this.timerContext = timerCtx;
}
	@Override
	public void run() {
		ByteBuf buf = null;
		try {
			Connection connection = null;
			PreparedStatement stmt = null;
			try {
				connection = DriverManager.getConnection(
						"jdbc:mysql://127.0.0.1:3306/netty?useSSL=false&autoReconnect=true&failOverReadOnly=false&maxReconnects=10",
						"root", "javawso2");
				Timestamp current = Timestamp.from(Instant.now()); // get current timestamp
				String sql = "INSERT INTO Timestamp (timestamp) VALUES (?)";
				stmt = connection.prepareStatement(sql);
				stmt.setTimestamp(1, current);
				stmt.executeUpdate();
				buf = Unpooled.copiedBuffer(current.toString().getBytes());
			} catch (Exception e) {
				AdaptiveConcurrencyControl.LOGGER.error("Exception", e);
			} finally {
				if (stmt != null) {
					try {
						stmt.close();
					} catch (Exception e) {
						AdaptiveConcurrencyControl.LOGGER.error("Exception", e);
					}
				}
				if (connection != null) {
					try {
						connection.close();
					} catch (Exception e) {
						AdaptiveConcurrencyControl.LOGGER.error("Exception", e);
					}
				}
			}
		} catch (Exception e) {
			AdaptiveConcurrencyControl.LOGGER.error("Exception in DbWrite Run method", e);
		}
		
		boolean keepAlive = HttpUtil.isKeepAlive(msg);
		FullHttpResponse response = null;
		try {
			response = new DefaultFullHttpResponse(HTTP_1_1, OK, buf);
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