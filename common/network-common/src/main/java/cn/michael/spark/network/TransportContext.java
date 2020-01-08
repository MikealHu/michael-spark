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
 *
 * 包含用于创建{@link TransportServer},{@link TransportClientFactory}以及使用
 * {@link TransportChannelHandler}设置Netty Channel管道的上下文。
 *
 * TransportClient提供两种通信协议，控制层的RPCs和数据层的"chunk fetching"。
 * RPC的处理是在TransportContext的范围之外执行的（即由用户提供的处理程序执行的），
 * 它负责设置可以使用零拷贝IO在数据层以块流式传输的流。
 *
 * TransportServer和TransportClientFactory都创建了一个针对每一个管道的TransportChannelHandler。
 * 由于每一个TransportChannelHandler包含了一个TransportClient，因此服务器进程可以利用现有管道将消息发送回客户端。
 */
public class TransportContext implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(TransportContext.class);

    private final TransportConf conf;
    private final RpcHandler rpcHandler;
    private final boolean closeIdleConnections;
    // Shuffle服务的注册连接数
    private Counter registeredConnections = new Counter();

    /**
     * 强制创建MessageEncoder和MessageDecoder，以便我们确保将当前上下文类加载器切好到ExecutorClassLoader之前创建他们。
     */
    private static final MessageEncoder ENCODER = MessageEncoder.INSTANCE;
    private static final MessageDecoder DECODER = MessageDecoder.INSTANCE;

    /**
     * 用于处理ChunkFetchRequest的单独线程池。 这有助于启用限制最大数量的TransportServer工作线程，
     * 这些线程在通过基础通道将ChunkFetchRequest消息的响应写回到客户端时被阻止。
     */
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
     * @param isClientOnly         该配置表示TransportContext仅通过一个客户端使用。
     *                             启动外部shuffle的时候，该配置是很重要的，它停止为shuffle客户端
     *                             创建额外的事件循环和后续线程池，以处理分块的提取请求。
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

    /**
     * 初始化运行给定TransportClientBootstraps的ClientFactory，然后返回一个新的客户端。
     * 引导程序必须同步执行，并且必须成功创建后才可以创建客户端。
     *
     * @param bootstraps
     * @return
     */
    public TransportClientFactory createClientFactory(List<TransportClientBootstrap> bootstraps) {
        return new TransportClientFactory(this, bootstraps);
    }

    public TransportClientFactory createClientFactory() {
        return createClientFactory(new ArrayList<>());
    }

    /** 创建一个服务器用于尝试连接具体端口 */
    public TransportServer createServer(int port, List<TransportServerBootstrap> bootstraps) {
        return new TransportServer(this, null, port, rpcHandler, bootstraps);
    }

    /** 创建一个服务器用于尝试连接具体主机地址+端口 */
    public TransportServer createServer(
            String host, int port, List<TransportServerBootstrap> bootstraps) {
        return new TransportServer(this, host, port, rpcHandler, bootstraps);
    }

    /** 创建一个新的服务器, 连接一些可用的临时端口。*/
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
     * 并具有{@link TransportChannelHandler}来处理请求或响应消息。
     * <p>
     * 来自网络的请求首先经过TransportFrameDecoder进行粘包拆包，然后将数据传递给MessageDecoder，
     * 由MessageDecoder进行解码，之后再经过IdleStateHandler来检查Channel空闲情况，
     * 最终将解码了的消息传递给TransportChannelHandler。
     * 在TransportChannelHandler中根据消息类型的不同转发给不同的处理方法进行处理，将消息发送给上层的代码。
     *
     * @param channel 需要初始化的管道
     * @param channelRpcHandler 用于管道的RPC 处理程序
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
