package cn.michael.spark.network.client;

import cn.michael.spark.network.TransportContext;
import cn.michael.spark.network.server.TransportChannelHandler;
import cn.michael.spark.network.util.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * author: hufenggang
 * email: hufenggang2019@gmail.com
 * date: 2019/12/26 11:34
 * <p>
 * 客户端工厂
 * <p>
 * Factory for creating {@link TransportClient}s by using createClient.
 * <p>
 * 工厂维度与其他主机的连接池，并应为同一远程主机返回相同的TransportClient
 * 它还为所有的TransportClient共享了一个工作线程池。
 * <p>
 * TransportClients将在可能的情况下被重用。
 * 在完成创建新的TransportClient之前，将运行所有给定的{@link TransportClientBootstrap}。
 */
public class TransportClientFactory implements Closeable {

    /** 客户端数据池 */
    private static class ClientPool {
        TransportClient[] clients;
        Object[] locks;

        ClientPool(int size) {
            clients = new TransportClient[size];
            locks = new Object[size];
            for (int i = 0; i < size; i++) {
                locks[i] = new Object();
            }
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(TransportClientFactory.class);

    private final TransportContext context;
    private final TransportConf conf;
    private final List<TransportClientBootstrap> clientBootstraps;
    private final ConcurrentHashMap<SocketAddress, ClientPool> connectionPool;

    /** 随机数生成器，用于选择对等体之间的连接 */
    private final Random rand;
    private final int numConnectionsPerPeer;

    private final Class<? extends Channel> socketChannelClass;
    private EventLoopGroup workerGroup;
    private final PooledByteBufAllocator pooledAllocator;
    private final NettyMemoryMetrics metrics;

    public TransportClientFactory(
            TransportContext context,
            List<TransportClientBootstrap> clientBootstraps) {
        this.context = Preconditions.checkNotNull(context);
        this.conf = context.getConf();
        this.clientBootstraps = Lists.newArrayList(Preconditions.checkNotNull(clientBootstraps));
        this.connectionPool = new ConcurrentHashMap<>();
        this.numConnectionsPerPeer = conf.numConnectionsPerPeer();
        this.rand = new Random();

        IOMode ioMode = IOMode.valueOf(conf.ioMode());
        this.socketChannelClass = NettyUtils.getClientChannelClass(ioMode);
        this.workerGroup = NettyUtils.createEventLoop(
                ioMode,
                conf.clientThreads(),
                conf.getModuleName() + "-client");
        if (conf.sharedByteBufAllocators()) {
            this.pooledAllocator = NettyUtils.getSharedPooledByteBufAllocator(
                    conf.preferDirectBufsForSharedByteBufAllocators(), false /* allowCache */);
        } else {
            this.pooledAllocator = NettyUtils.createPooledByteBufAllocator(
                    conf.preferDirectBufs(), false /* allowCache */, conf.clientThreads());
        }
        this.metrics = new NettyMemoryMetrics(
                this.pooledAllocator, conf.getModuleName() + "-client", conf);
    }

    @Override
    public void close() throws IOException {

    }

    /**
     * @param remoteHost 远程地址
     * @param remotePort 远程端口
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public TransportClient createClient(String remoteHost, int remotePort) throws IOException, InterruptedException {
        // 首先从连接池获取连接，如果找不到或者不活跃，则新建一个。
        // 每次创建客户端时，请在此处使用未解析的地址以避免DNS解析。
        final InetSocketAddress unresolvedAddress = InetSocketAddress.createUnresolved(remoteHost, remotePort);

        // 如果还没有ClientPool，则创建一个ClientPool
        ClientPool clientPool = connectionPool.get(unresolvedAddress);
        if (clientPool == null) {
            connectionPool.putIfAbsent(unresolvedAddress, new ClientPool(numConnectionsPerPeer));
            clientPool = connectionPool.get(unresolvedAddress);
        }

        int clientIndex = rand.nextInt(numConnectionsPerPeer);
        TransportClient cachedClient = clientPool.clients[clientIndex];

        if (cachedClient != null && cachedClient.isActive()) {

            TransportChannelHandler handler = cachedClient.getChannel().pipeline()
                    .get(TransportChannelHandler.class);
            synchronized (handler) {
                handler.getResponseHandler().updateTimeOfLastRequest();
            }
        }

        return null;
    }
}
