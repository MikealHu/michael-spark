package cn.michael.spark.network.client;

import cn.michael.spark.network.buffer.NioManagedBuffer;
import cn.michael.spark.network.protocol.ChunkFetchRequest;
import cn.michael.spark.network.protocol.RpcRequest;
import cn.michael.spark.network.protocol.StreamChunkId;
import cn.michael.spark.network.protocol.StreamRequest;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SettableFuture;
import com.sun.istack.internal.Nullable;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static cn.michael.spark.network.util.NettyUtils.getRemoteAddress;

/**
 * author: hufenggang
 * email: hufenggang2019@gmail.com
 * date: 2019/12/25 9:55
 */
public class TransportClient implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(TransportClient.class);

    private final Channel channel;
    private final TransportResponseHandler handler;
    @Nullable
    private String clientId;
    private volatile boolean timedOut;

    public TransportClient(Channel channel, TransportResponseHandler handler) {
        this.channel = Preconditions.checkNotNull(channel);
        this.handler = Preconditions.checkNotNull(handler);
        this.timedOut = false;
    }

    public Channel getChannel() {
        return channel;
    }

    public boolean isActive() {
        return !timedOut && (channel.isOpen() || channel.isActive());
    }

    public SocketAddress getSocketAddress() {
        return channel.remoteAddress();
    }

    /**
     * 返回启用身份验证时客户端用于对其自身进行身份验证的客户端编号。
     *
     * @return 如果禁用身份校验则为null
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * 设置经由身份验证的客户端编号，这个应由身份验证层使用。
     * <p>
     * 在已经设置了客户端编号后再次设置不同的客户端编号会报错。
     */
    public void setClientId(String id) {
        Preconditions.checkState(clientId == null, "Client ID has already been set.");
        this.clientId = id;
    }

    /**
     * 通过给定的streamId，获取远端数据流
     *
     * @param streamId The stream to fetch.
     * @param callback Object to call with the stream data.
     */
    public void stream(String streamId, StreamCallback callback) {
        StdChannelListener listener = new StdChannelListener(streamId) {
            @Override
            void handleFailure(String errorMsg, Throwable cause) throws Exception {
                callback.onFailure(streamId, new IOException(errorMsg, cause));
            }
        };
        if (logger.isDebugEnabled()) {
            logger.debug("Sending stream request for {} to {}", streamId, getRemoteAddress(channel));
        }

        synchronized (this) {
            handler.addStreamCallback(streamId, callback);
            channel.writeAndFlush(new StreamRequest(streamId)).addListener(listener);
        }
    }

    /**
     * 从远端或者预先定义的streamId请求单个块。
     * <p>
     * 块索引从0开始，多次请求相同的块是有效的，某些流可能不支持此功能。
     * <p>
     * 多个fetchChunk请求可能同时未完成，
     * 假设仅使用一个TransportClient来获取块，则可以保证以与请求时相同的顺序返回这些块。
     *
     * @param streamId   引导远端StreamManager流的标识符。该编号需要事先由客户端和服务器同意
     * @param chunkIndex 从0开始的块索引
     * @param callback   成功接收到数据块或者发生失败的时候
     */
    public void fetchChunk(
            long streamId,
            int chunkIndex,
            ChunkReceivedCallback callback) {
        if (logger.isDebugEnabled()) {
            logger.debug("Sending fetch chunk request {} to {}", chunkIndex, getRemoteAddress(channel));
        }

        StreamChunkId streamChunkId = new StreamChunkId(streamId, chunkIndex);
        StdChannelListener listener = new StdChannelListener(streamChunkId) {
            @Override
            void handleFailure(String errorMsg, Throwable cause) {
                handler.removeFetchRequest(streamChunkId);
                callback.onFailure(chunkIndex, new IOException(errorMsg, cause));
            }
        };
        handler.addFetchRequest(streamChunkId, callback);

        channel.writeAndFlush(new ChunkFetchRequest(streamChunkId)).addListener(listener);
    }

    /**
     * 将不透明消息发送到服务器端的RpcHandler。
     * 该回调将在服务器的响应或任何失败时被调用。
     *
     * @param message  需要发送的消息
     * @param callback 回调用于处理Rpc的回复
     * @return
     */
    public long sendRpc(ByteBuffer message, RpcResponseCallback callback) {
        if (logger.isTraceEnabled()) {
            logger.trace("Sending RPC to {}", getRemoteAddress(channel));
        }

        long requestId = requestId();
        handler.addRpcRequest(requestId, callback);

        RpcChannelListener listener = new RpcChannelListener(requestId, callback);
        channel.writeAndFlush(new RpcRequest(requestId, new NioManagedBuffer(message)))
                .addListener(listener);

        return requestId;
    }

    /**
     * 同步发送不透明消息到服务器端的RpcHandler，等待指定的超时时间以响应。
     *
     * @param message
     * @param timeoutMs
     * @return
     */
    public ByteBuffer sendRpcSync(ByteBuffer message, long timeoutMs) {
        final SettableFuture<ByteBuffer> result = SettableFuture.create();

        sendRpc(message, new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
                try {
                    ByteBuffer copy = ByteBuffer.allocate(response.remaining());
                    copy.put(response);
                    copy.flip();
                    result.set(copy);
                } catch (Throwable t) {
                    logger.warn("Error in responding PRC callback", t);
                    result.setException(t);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                result.setException(e);
            }
        });

        try {
            return result.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void close() throws IOException {
        channel.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("remoteAdress", channel.remoteAddress())
                .add("clientId", clientId)
                .add("isActive", isActive())
                .toString();
    }

    private static long requestId() {
        return Math.abs(UUID.randomUUID().getLeastSignificantBits());
    }

    private class StdChannelListener
            implements GenericFutureListener<Future<? super Void>> {
        final long startTime;

        final Object requestId;

        StdChannelListener(Object requestId) {
            this.startTime = System.currentTimeMillis();
            this.requestId = requestId;
        }

        @Override
        public void operationComplete(Future<? super Void> future) throws Exception {
            if (future.isSuccess()) {
                if (logger.isTraceEnabled()) {
                    long timeTaken = System.currentTimeMillis() - startTime;
                    logger.trace("Sending request {} to {} took {} ms", requestId,
                            getRemoteAddress(channel), timeTaken);
                }
            } else {
                String errorMsg = String.format("Failed to send RPC %s to %s: %s", requestId,
                        getRemoteAddress(channel), future.cause());
                logger.error(errorMsg, future.cause());
                channel.close();
                try {
                    handleFailure(errorMsg, future.cause());
                } catch (Exception e) {
                    logger.error("Uncaught exception in RPC response callback handler!", e);
                }
            }
        }

        void handleFailure(String errorMsg, Throwable cause) throws Exception {
        }


    }

    private class RpcChannelListener extends StdChannelListener {
        final long rpcRequestId;
        final RpcResponseCallback callback;

        RpcChannelListener(long rpcRequestId, RpcResponseCallback callback) {
            super("RPC " + rpcRequestId);
            this.rpcRequestId = rpcRequestId;
            this.callback = callback;
        }

        @Override
        void handleFailure(String errorMsg, Throwable cause) {
            handler.removeRpcRequest(rpcRequestId);
            callback.onFailure(new IOException(errorMsg, cause));
        }
    }

    public void timeOut() {
        this.timedOut = true;
    }
}
