package cn.michael.spark.network.client;

import cn.michael.spark.network.protocol.*;
import cn.michael.spark.network.server.MessageHandler;
import cn.michael.spark.network.util.TransportFrameDecoder;
import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.Channel;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import static cn.michael.spark.network.util.NettyUtils.getRemoteAddress;

/**
 * 处理服务器响应以响应[[TransportClient]]发出的请求的处理程序。
 * 它通过跟踪未完成的请求（及其回调）列表来工作。
 * <p>
 * 并发：线程安全，可以从多个线程中调用。
 */
public class TransportResponseHandler extends MessageHandler<ResponseMessage> {
    private static final Logger logger = LoggerFactory.getLogger(TransportResponseHandler.class);

    private final Channel channel;

    private final Map<StreamChunkId, ChunkReceivedCallback> outstandingFetches;

    private final Map<Long, RpcResponseCallback> outstandingRpcs;

    private final Queue<Pair<String, StreamCallback>> streamCallbacks;
    private volatile boolean streamActive;

    /**
     * Records the time (in system nanoseconds) that the last fetch or RPC request was sent.
     */
    private final AtomicLong timeOfLastRequestNs;

    public TransportResponseHandler(Channel channel) {
        this.channel = channel;
        this.outstandingFetches = new ConcurrentHashMap<>();
        this.outstandingRpcs = new ConcurrentHashMap<>();
        this.streamCallbacks = new ConcurrentLinkedQueue<>();
        this.timeOfLastRequestNs = new AtomicLong(0);
    }

    public void addFetchRequest(StreamChunkId streamChunkId, ChunkReceivedCallback callback) {
        updateTimeOfLastRequest();
        outstandingFetches.put(streamChunkId, callback);
    }

    public void removeFetchRequest(StreamChunkId streamChunkId) {
        outstandingFetches.remove(streamChunkId);
    }

    public void addRpcRequest(long requestId, RpcResponseCallback callback) {
        updateTimeOfLastRequest();
        outstandingRpcs.put(requestId, callback);
    }

    public void removeRpcRequest(long requestId) {
        outstandingRpcs.remove(requestId);
    }

    public void addStreamCallback(String streamId, StreamCallback callback) {
        updateTimeOfLastRequest();
        streamCallbacks.offer(ImmutablePair.of(streamId, callback));
    }

    @VisibleForTesting
    public void deactivateStream() {
        streamActive = false;
    }

    /**
     * 对所有未完成的请求触发失败回调。 当我们有未捕获的异常或过早的连接终止时，将调用此方法。
     */
    private void failOutstandingRequests(Throwable cause) {
        for (Map.Entry<StreamChunkId, ChunkReceivedCallback> entry : outstandingFetches.entrySet()) {
            try {
                entry.getValue().onFailure(entry.getKey().chunkIndex, cause);
            } catch (Exception e) {
                logger.warn("ChunkReceivedCallback.onFailure throws exception", e);
            }
        }
        for (Map.Entry<Long, RpcResponseCallback> entry : outstandingRpcs.entrySet()) {
            try {
                entry.getValue().onFailure(cause);
            } catch (Exception e) {
                logger.warn("RpcResponseCallback.onFailure throws exception", e);
            }
        }
        for (Pair<String, StreamCallback> entry : streamCallbacks) {
            try {
                entry.getValue().onFailure(entry.getKey(), cause);
            } catch (Exception e) {
                logger.warn("StreamCallback.onFailure throws exception", e);
            }
        }

        // It's OK if new fetches appear, as they will fail immediately.
        outstandingFetches.clear();
        outstandingRpcs.clear();
        streamCallbacks.clear();
    }

    @Override
    public void channelActive() {
    }

    @Override
    public void channelInactive() {
        if (numOutstandingRequests() > 0) {
            String remoteAddress = getRemoteAddress(channel);
            logger.error("Still have {} requests outstanding when connection from {} is closed",
                    numOutstandingRequests(), remoteAddress);
            failOutstandingRequests(new IOException("Connection from " + remoteAddress + " closed"));
        }
    }

    @Override
    public void exceptionCaught(Throwable cause) {
        if (numOutstandingRequests() > 0) {
            String remoteAddress = getRemoteAddress(channel);
            logger.error("Still have {} requests outstanding when connection from {} is closed",
                    numOutstandingRequests(), remoteAddress);
            failOutstandingRequests(cause);
        }
    }

    @Override
    public void handle(ResponseMessage message) throws Exception {
        if (message instanceof ChunkFetchSuccess) {
            processChunkFetchSuccess(message);
        } else if (message instanceof ChunkFetchFailure) {
            processChunkFetchFailure(message);
        } else if (message instanceof RpcResponse) {
            processRpcRequest(message);
        } else if (message instanceof RpcFailure) {
            processRpcFailure(message);
        } else if (message instanceof StreamResponse) {
            processStreamResponse(message);
        } else if (message instanceof StreamFailure) {
            processStreamFailure(message);
        } else {
            throw new IllegalStateException("Unknown response type: " + message.type());
        }
    }

    private void processStreamFailure(ResponseMessage message) {
        StreamFailure resp = (StreamFailure) message;
        Pair<String, StreamCallback> entry = streamCallbacks.poll();
        if (entry != null) {
            StreamCallback callback = entry.getValue();
            try {
                callback.onFailure(resp.streamId, new RuntimeException(resp.error));
            } catch (IOException ioe) {
                logger.warn("Error in stream failure handler.", ioe);
            }
        } else {
            logger.warn("Stream failure with unknown callback: {}", resp.error);
        }
    }

    private void processChunkFetchFailure(ResponseMessage message) {
        ChunkFetchFailure resp = (ChunkFetchFailure) message;
        ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
        if (listener == null) {
            logger.warn("Ignoring response for block {} from {} ({}) since it is not outstanding",
                    resp.streamChunkId, getRemoteAddress(channel), resp.errorString);
        } else {
            outstandingFetches.remove(resp.streamChunkId);
            listener.onFailure(resp.streamChunkId.chunkIndex, new ChunkFetchFailureException(
                    "Failure while fetching " + resp.streamChunkId + ": " + resp.errorString));
        }
    }

    private void processChunkFetchSuccess(ResponseMessage message) {
        ChunkFetchSuccess resp = (ChunkFetchSuccess) message;
        ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
        if (listener == null) {
            logger.warn("Ignoring response for block {} from {} since it is not outstanding",
                    resp.streamChunkId, getRemoteAddress(channel));
            resp.body().release();
        } else {
            outstandingFetches.remove(resp.streamChunkId);
            listener.onSuccess(resp.streamChunkId.chunkIndex, resp.body());
            resp.body().release();
        }
    }

    private void processStreamResponse(ResponseMessage message) {
        StreamResponse resp = (StreamResponse) message;
        Pair<String, StreamCallback> entry = streamCallbacks.poll();
        if (entry != null) {
            StreamCallback callback = entry.getValue();
            if (resp.byteCount > 0) {
                StreamInterceptor<ResponseMessage> interceptor = new StreamInterceptor<>(
                        this, resp.streamId, resp.byteCount, callback);
                try {
                    TransportFrameDecoder frameDecoder = (TransportFrameDecoder)
                            channel.pipeline().get(TransportFrameDecoder.HANDLER_NAME);
                    frameDecoder.setInterceptor(interceptor);
                    streamActive = true;
                } catch (Exception e) {
                    logger.error("Error installing stream handler.", e);
                    deactivateStream();
                }
            } else {
                try {
                    callback.onComplete(resp.streamId);
                } catch (Exception e) {
                    logger.warn("Error in stream handler onComplete().", e);
                }
            }
        } else {
            logger.error("Could not find callback for StreamResponse.");
        }
    }

    private void processRpcFailure(ResponseMessage message) {
        RpcFailure resp = (RpcFailure) message;
        RpcResponseCallback listener = outstandingRpcs.get(resp.requestId);
        if (listener == null) {
            logger.warn("Ignoring response for RPC {} from {} ({}) since it is not outstanding",
                    resp.requestId, getRemoteAddress(channel), resp.errorString);
        } else {
            outstandingRpcs.remove(resp.requestId);
            listener.onFailure(new RuntimeException(resp.errorString));
        }
    }

    private void processRpcRequest(Message message) {
        RpcResponse resp = (RpcResponse) message;
        RpcResponseCallback listener = outstandingRpcs.get(resp.requestId);
        if (listener == null) {
            logger.warn("Ignoring response for RPC {} from {} ({} bytes) since it is not outstanding",
                    resp.requestId, getRemoteAddress(channel), resp.body().size());
        } else {
            outstandingRpcs.remove(resp.requestId);
            try {
                listener.onSuccess(resp.body().nioByteBuffer());
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                resp.body().release();
            }
        }
    }

    /**
     * Returns total number of outstanding requests (fetch requests + rpcs)
     */
    public int numOutstandingRequests() {
        return outstandingFetches.size() + outstandingRpcs.size() + streamCallbacks.size() +
                (streamActive ? 1 : 0);
    }

    /**
     * Returns the time in nanoseconds of when the last request was sent out.
     */
    public long getTimeOfLastRequestNs() {
        return timeOfLastRequestNs.get();
    }

    /**
     * Updates the time of the last request to the current system time.
     */
    public void updateTimeOfLastRequest() {
        timeOfLastRequestNs.set(System.nanoTime());
    }

}
