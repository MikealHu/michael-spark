package cn.michael.spark.network.server;

import cn.michael.spark.network.TransportContext;
import cn.michael.spark.network.client.TransportClient;
import cn.michael.spark.network.client.TransportResponseHandler;
import cn.michael.spark.network.protocol.ChunkFetchRequest;
import cn.michael.spark.network.protocol.Message;
import cn.michael.spark.network.protocol.RequestMessage;
import cn.michael.spark.network.protocol.ResponseMessage;
import cn.michael.spark.network.util.NettyUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 单个传输级通道处理程序，用于将请求委派给{@link TransportRequestHandler}和对{@link TransportResponseHandler}的响应。
 * <p>
 * 在传输层中创建的所有通道都是双向的。 当客户端使用RequestMessage（由服务器的RequestHandler处理）启动Netty通道时，
 * 服务器将生成ResponseMessage（由客户端的ResponseHandler处理）。 但是，服务器还在同一个通道上获得一个句柄，
 * 因此它可能随后开始向客户端发送RequestMessages。
 * 这意味着客户端也需要一个RequestHandler，服务器也需要一个ResponseHandler，以便客户端对服务器请求的响应。
 * <p>
 * 此类还处理来自{@link io.netty.handler.timeout.IdleStateHandler}的超时。 如果存在未完成的抓取或RPC请求，
 * 但通道上至少“ requestTimeoutMs”没有流量，则我们认为连接已超时。 请注意，这是双工流量。 为简单起见，
 * 如果客户端持续发送但没有响应，我们不会超时。
 */
public class TransportChannelHandler extends SimpleChannelInboundHandler<Message> {
    private static final Logger logger = LoggerFactory.getLogger(TransportChannelHandler.class);

    private final TransportClient client;
    private final TransportResponseHandler responseHandler;
    private final TransportRequestHandler requestHandler;
    private final long requestTimeoutNs;
    private final boolean closeIdleConnections;
    private final TransportContext transportContext;

    public TransportChannelHandler(
            TransportClient client,
            TransportResponseHandler responseHandler,
            TransportRequestHandler requestHandler,
            long requestTimeoutMs,
            boolean closeIdleConnections,
            TransportContext transportContext) {
        this.client = client;
        this.responseHandler = responseHandler;
        this.requestHandler = requestHandler;
        this.requestTimeoutNs = requestTimeoutMs * 1000L * 1000;
        this.closeIdleConnections = closeIdleConnections;
        this.transportContext = transportContext;
    }

    public TransportClient getClient() {
        return client;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.warn("Exception in connection from " + NettyUtils.getRemoteAddress(ctx.channel()),
                cause);
        requestHandler.exceptionCaught(cause);
        responseHandler.exceptionCaught(cause);
        ctx.close();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        try {
            requestHandler.channelActive();
        } catch (RuntimeException e) {
            logger.error("Exception from request handler while channel is active", e);
        }
        try {
            responseHandler.channelActive();
        } catch (RuntimeException e) {
            logger.error("Exception from response handler while channel is active", e);
        }
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        try {
            requestHandler.channelInactive();
        } catch (RuntimeException e) {
            logger.error("Exception from request handler while channel is inactive", e);
        }
        try {
            responseHandler.channelInactive();
        } catch (RuntimeException e) {
            logger.error("Exception from response handler while channel is inactive", e);
        }
        super.channelInactive(ctx);
    }

    /**
     * 覆盖acceptInboundMessage以将ChunkFetchRequest消息正确委派给ChunkFetchRequestHandler。
     */
    @Override
    public boolean acceptInboundMessage(Object msg) throws Exception {
        if (msg instanceof ChunkFetchRequest) {
            return false;
        } else {
            return super.acceptInboundMessage(msg);
        }
    }

    /**
     * 根据消息类型的不同，将请求消息转发给TrannsportRequestHandler，将响应消息转发给TransportResponseHandler。
     *
     * @param ctx     ChannelHandlerContext对象
     * @param message 消息
     * @throws Exception
     */
    @Override
    public void channelRead0(ChannelHandlerContext ctx, Message message) throws Exception {
        if (message instanceof RequestMessage) {
            requestHandler.handle((RequestMessage) message);
        } else if (message instanceof ResponseMessage) {
            responseHandler.handle((ResponseMessage) message);
        } else {
            ctx.fireChannelRead(message);
        }
    }

    /**
     * Triggered based on events from an {@link io.netty.handler.timeout.IdleStateHandler}.
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            // See class comment for timeout semantics. In addition to ensuring we only timeout while
            // there are outstanding requests, we also do a secondary consistency check to ensure
            // there's no race between the idle timeout and incrementing the numOutstandingRequests
            // (see SPARK-7003).
            //
            // To avoid a race between TransportClientFactory.createClient() and this code which could
            // result in an inactive client being returned, this needs to run in a synchronized block.
            synchronized (this) {
                boolean hasInFlightRequests = responseHandler.numOutstandingRequests() > 0;
                boolean isActuallyOverdue =
                        System.nanoTime() - responseHandler.getTimeOfLastRequestNs() > requestTimeoutNs;
                if (e.state() == IdleState.ALL_IDLE && isActuallyOverdue) {
                    if (hasInFlightRequests) {
                        String address = NettyUtils.getRemoteAddress(ctx.channel());
                        logger.error("Connection to {} has been quiet for {} ms while there are outstanding " +
                                "requests. Assuming connection is dead; please adjust spark.network.timeout if " +
                                "this is wrong.", address, requestTimeoutNs / 1000 / 1000);
                        client.timeOut();
                        ctx.close();
                    } else if (closeIdleConnections) {
                        // While CloseIdleConnections is enable, we also close idle connection
                        client.timeOut();
                        ctx.close();
                    }
                }
            }
        }
        ctx.fireUserEventTriggered(evt);
    }

    public TransportResponseHandler getResponseHandler() {
        return responseHandler;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        transportContext.getRegisteredConnections().inc();
        super.channelRegistered(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        transportContext.getRegisteredConnections().dec();
        super.channelUnregistered(ctx);
    }
}
