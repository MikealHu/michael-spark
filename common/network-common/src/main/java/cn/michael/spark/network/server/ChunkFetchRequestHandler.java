package cn.michael.spark.network.server;

import cn.michael.spark.network.buffer.ManagedBuffer;
import cn.michael.spark.network.client.TransportClient;
import cn.michael.spark.network.protocol.ChunkFetchFailure;
import cn.michael.spark.network.protocol.ChunkFetchRequest;
import cn.michael.spark.network.protocol.ChunkFetchSuccess;
import cn.michael.spark.network.protocol.Encodable;
import com.google.common.base.Throwables;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;

import static cn.michael.spark.network.util.NettyUtils.getRemoteAddress;

/**
 * author: hufenggang
 * email: hufenggang2019@gmail.com
 * date: 2020/1/2 9:58
 */
public class ChunkFetchRequestHandler extends SimpleChannelInboundHandler<ChunkFetchRequest> {
    private static final Logger logger = LoggerFactory.getLogger(ChunkFetchRequestHandler.class);

    private final TransportClient client;
    private final StreamManager streamManager;
    // 正在传输但尚未完成的最大块数
    private final long maxChunksBeingTransferred;

    public ChunkFetchRequestHandler(
            TransportClient client,
            StreamManager streamManager,
            Long maxChunksBeingTransferred) {
        this.client = client;
        this.streamManager = streamManager;
        this.maxChunksBeingTransferred = maxChunksBeingTransferred;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.warn("Exception in connection from " + getRemoteAddress(ctx.channel()), cause);
        ctx.close();
    }

    @Override
    protected void channelRead0(
            ChannelHandlerContext ctx,
            final ChunkFetchRequest msg) throws Exception {
        Channel channel = ctx.channel();
        if (logger.isTraceEnabled()) {
            logger.trace("Received req from {} to fetch block {}", getRemoteAddress(channel), msg.streamChunkId);
        }
        long chunksBeingTransferred = streamManager.chunksBeingTransferred();
        if (chunksBeingTransferred >= maxChunksBeingTransferred) {
            logger.warn("The number of chunks being transferred {} is above {}, close the connection.",
                    chunksBeingTransferred, maxChunksBeingTransferred);
            channel.close();
            return;
        }
        ManagedBuffer buf;
        try {
            streamManager.checkAuthorization(client, msg.streamChunkId.streamId);
            buf = streamManager.getChunk(msg.streamChunkId.streamId, msg.streamChunkId.chunkIndex);
            if (buf == null) {
                throw new IllegalStateException("Chunk was not found");
            }
        } catch (Exception e) {
            logger.error(String.format("Error opening block %s for request from %s",
                    msg.streamChunkId, getRemoteAddress(channel)), e);
            respond(channel, new ChunkFetchFailure(msg.streamChunkId,
                    Throwables.getStackTraceAsString(e)));
            return;
        }

        streamManager.chunkBeingSent(msg.streamChunkId.streamId);
        respond(channel, new ChunkFetchSuccess(msg.streamChunkId, buf)).addListener(
                (ChannelFutureListener) future -> streamManager.chunkSent(msg.streamChunkId.streamId));
    }

    private ChannelFuture respond(
            final Channel channel,
            final Encodable result) throws InterruptedException {
        final SocketAddress remoteAddress = channel.remoteAddress();
        return channel.writeAndFlush(result).await().addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                logger.trace("Sent result {} to client {}", result, remoteAddress);
            } else {
                logger.error(String.format("Error sending result %s to %s; closing connection",
                        result, remoteAddress), future.cause());
                channel.close();
            }
        });
    }
}
