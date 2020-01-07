package cn.michael.spark.network;

import cn.michael.spark.network.client.TransportClient;
import cn.michael.spark.network.client.TransportClientBootstrap;
import cn.michael.spark.network.client.TransportClientFactory;
import cn.michael.spark.network.client.TransportResponseHandler;
import cn.michael.spark.network.protocol.MessageDecoder;
import cn.michael.spark.network.protocol.MessageEncoder;
import cn.michael.spark.network.server.*;
import cn.michael.spark.network.util.IOMode;
import cn.michael.spark.network.util.NettyUtils;
import cn.michael.spark.network.util.TransportConf;
import cn.michael.spark.network.util.TransportFrameDecoder;
import com.codahale.metrics.Counter;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

/**
 * author: hufenggang
 * email: hufenggang2019@gmail.com
 * date: 2019/12/26 11:37
 */
public class TransportContext implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(TransportContext.class);

    private final TransportConf conf;
    private final RpcHandler rpcHandler;
    private final boolean closeIdleConnections;
    // Shuffle服务的注册连接数
    private Counter registeredConnections = new Counter();

    private static final MessageEncoder ENCODER = MessageEncoder.INSTANCE;
    private static final MessageDecoder DECODER = MessageDecoder.INSTANCE;

    private final EventLoopGroup chunkFetchWorkers;

    public TransportContext(TransportConf conf, RpcHandler rpcHandler) {
        this(conf, rpcHandler, false, false);
    }

    public TransportContext(
            TransportConf conf,
            RpcHandler rpcHandler,
            boolean closeIdleConnections) {
        this(conf, rpcHandler, closeIdleConnections, false);
    }

    /**
     * 为基础客户端和服务器启用TransportContext初始化。
     *
     * @param conf                 TransportConf对象
     * @param rpcHandler           RpcHandler负责处理请求和响应
     * @param closeIdleConnections 如果设置为true，则关闭空闲连接
     * @param isClientOnly
     */
    public TransportContext(
            TransportConf conf,
            RpcHandler rpcHandler,
            boolean closeIdleConnections,
            boolean isClientOnly) {
        this.conf = conf;
        this.rpcHandler = rpcHandler;
        this.closeIdleConnections = closeIdleConnections;

        if (conf.getModuleName() != null &&
                conf.getModuleName().equalsIgnoreCase("shuffle") &&
                !isClientOnly) {
            chunkFetchWorkers = NettyUtils.createEventLoop(
                    IOMode.valueOf(conf.ioMode()),
                    conf.chunkFetchHandlerThreads(),
                    "shuffle-chunk-fetch-handler");
        } else {
            chunkFetchWorkers = null;
        }
    }

    public TransportClientFactory createClientFactory(List<TransportClientBootstrap> bootstraps) {
        return new TransportClientFactory(this, bootstraps);
    }

    public TransportClientFactory createClientFactory() {
        return createClientFactory(new ArrayList<>());
    }

    public TransportServer createServer(int port, List<TransportServerBootstrap> bootstraps) {
        return new TransportServer(this, null, port, rpcHandler, bootstraps);
    }

    public TransportServer createServer(
            String host, int port, List<TransportServerBootstrap> bootstraps) {
        return new TransportServer(this, host, port, rpcHandler, bootstraps);
    }

    /**
     * Creates a new server, binding to any available ephemeral port.
     */
    public TransportServer createServer(List<TransportServerBootstrap> bootstraps) {
        return createServer(0, bootstraps);
    }

    public TransportServer createServer() {
        return createServer(0, new ArrayList<>());
    }

    public TransportChannelHandler initializePipeline(SocketChannel channel) {
        return initializePipeline(channel, rpcHandler);
    }

    /**
     * 初始化客户端或服务器Netty Channel管道，该管道对消息进行编码/解码，
     * 并具有{@link cn.michael.spark.network.server.TransportChannelHandler}来处理请求或响应消息。
     * <p>
     * 来自网络的请求首先经过TransportFrameDecoder进行粘包拆包，然后将数据传递给MessageDecoder，
     * 由MessageDecoder进行解码，之后再经过IdleStateHandler来检查Channel空闲情况，
     * 最终将解码了的消息传递给TransportChannelHandler。
     * 在TransportChannelHandler中根据消息类型的不同转发给不同的处理方法进行处理，将消息发送给上层的代码。
     *
     * @param channel
     * @param channelRpcHandler
     * @return 返回创建的TransportChannelHandler，该对象包括一个被用于在管道上消息传递的TransportClient
     * TransportClient直接与ChannelHandler关联，以确保同一通道的所有用户都获得相同的TransportClient对象。
     */
    public TransportChannelHandler initializePipeline(
            SocketChannel channel,
            RpcHandler channelRpcHandler) {
        try {
            TransportChannelHandler channelHandler = createChannelHandler(channel, channelRpcHandler);
            ChunkFetchRequestHandler chunkFetchHandler =
                    createChunkFetchHandler(channelHandler, channelRpcHandler);
            ChannelPipeline pipeline = channel.pipeline()
                    .addLast("encoder", ENCODER)
                    .addLast(TransportFrameDecoder.HANDLER_NAME, NettyUtils.createFrameDecoder())
                    .addLast("decoder", DECODER)
                    .addLast("idleStateHandler",
                            new IdleStateHandler(0, 0, conf.connectionTimeoutMs() / 1000))
                    // NOTE: Chunks are currently guaranteed to be returned in the order of request, but this
                    // would require more logic to guarantee if this were not part of the same event loop.
                    .addLast("handler", channelHandler);
            // Use a separate EventLoopGroup to handle ChunkFetchRequest messages for shuffle rpcs.
            if (chunkFetchWorkers != null) {
                pipeline.addLast(chunkFetchWorkers, "chunkFetchHandler", chunkFetchHandler);
            }
            return channelHandler;
        } catch (RuntimeException e) {
            logger.error("Error while initializing Netty pipeline", e);
            throw e;
        }
    }

    private TransportChannelHandler createChannelHandler(Channel channel, RpcHandler rpcHandler) {
        TransportResponseHandler responseHandler = new TransportResponseHandler(channel);
        TransportClient client = new TransportClient(channel, responseHandler);
        TransportRequestHandler requestHandler = new TransportRequestHandler(channel, client,
                rpcHandler, conf.maxChunksBeingTransferred());
        return new TransportChannelHandler(client, responseHandler, requestHandler,
                conf.connectionTimeoutMs(), closeIdleConnections, this);
    }

    private ChunkFetchRequestHandler createChunkFetchHandler(TransportChannelHandler channelHandler,
                                                             RpcHandler rpcHandler) {
        return new ChunkFetchRequestHandler(channelHandler.getClient(), rpcHandler.getStreamManager(), conf.maxChunksBeingTransferred());
    }

    public TransportConf getConf() {
        return conf;
    }

    public Counter getRegisteredConnections() {
        return registeredConnections;
    }

    public void close() {
        if (chunkFetchWorkers != null) {
            chunkFetchWorkers.shutdownGracefully();
        }
    }
}
