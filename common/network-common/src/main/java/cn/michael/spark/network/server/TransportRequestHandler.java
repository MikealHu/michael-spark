package cn.michael.spark.network.server;

import cn.michael.spark.network.buffer.ManagedBuffer;
import cn.michael.spark.network.buffer.NioManagedBuffer;
import cn.michael.spark.network.client.RpcResponseCallback;
import cn.michael.spark.network.client.StreamCallbackWithID;
import cn.michael.spark.network.client.StreamInterceptor;
import cn.michael.spark.network.client.TransportClient;
import cn.michael.spark.network.protocol.*;
import cn.michael.spark.network.util.TransportFrameDecoder;
import com.google.common.base.Throwables;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

import static cn.michael.spark.network.util.NettyUtils.getRemoteAddress;

/**
 * 该处理程序处理来自客户端的请求并写回块数据。
 * 每个处理程序都附加到一个Netty通道，并跟踪通过该通道获取了哪些流，以便在终止该通道时对其进行清理。
 *
 * 消息应该已经由{@link TransportServer}的管道设置处理过
 */
public class TransportRequestHandler extends MessageHandler<RequestMessage> {
    private static final Logger logger = LoggerFactory.getLogger(TransportRequestHandler.class);

    // 该处理程序关联的Netty管道
    private final Channel channel;
    private final TransportClient reverseClient;
    private final RpcHandler rpcHandler;
    private final StreamManager streamManager;
    private final long maxChunksBeingTransferred;

    public TransportRequestHandler(
            Channel channel,
            TransportClient reverseClient,
            RpcHandler rpcHandler,
            Long maxChunksBeingTransferred) {
        this.channel = channel;
        this.reverseClient = reverseClient;
        this.rpcHandler = rpcHandler;
        this.streamManager = rpcHandler.getStreamManager();
        this.maxChunksBeingTransferred = maxChunksBeingTransferred;
    }


    @Override
    public void handle(RequestMessage request) throws Exception {
        if (request instanceof RpcRequest) {
            processRpcRequest((RpcRequest) request);
        } else if (request instanceof OneWayMessage) {
            processOneWayMessage((OneWayMessage) request);
        } else if (request instanceof StreamRequest) {
            processStreamRequest((StreamRequest) request);
        } else if (request instanceof UploadStream) {
            processStreamUpload((UploadStream) request);
        } else {
            throw new IllegalArgumentException("Unknown request type: " + request);
        }
    }

    private void processStreamUpload(UploadStream req) {
        assert (req.body() == null);
        try {
            RpcResponseCallback callback = new RpcResponseCallback() {
                @Override
                public void onSuccess(ByteBuffer response) {
                    respond(new RpcResponse(req.requestId, new NioManagedBuffer(response)));
                }

                @Override
                public void onFailure(Throwable e) {
                    respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
                }
            };
            TransportFrameDecoder frameDecoder = (TransportFrameDecoder)
                    channel.pipeline().get(TransportFrameDecoder.HANDLER_NAME);
            ByteBuffer meta = req.meta.nioByteBuffer();
            StreamCallbackWithID streamHandler = rpcHandler.receiveStream(reverseClient, meta, callback);
            if (streamHandler == null) {
                throw new NullPointerException("rpcHandler returned a null streamHandler");
            }
            StreamCallbackWithID wrappedCallback = new StreamCallbackWithID() {
                @Override
                public void onData(String streamId, ByteBuffer buf) throws IOException {
                    streamHandler.onData(streamId, buf);
                }

                @Override
                public void onComplete(String streamId) throws IOException {
                    try {
                        streamHandler.onComplete(streamId);
                        callback.onSuccess(ByteBuffer.allocate(0));
                    } catch (Exception ex) {
                        IOException ioExc = new IOException("Failure post-processing complete stream;" +
                                " failing this rpc and leaving channel active", ex);
                        callback.onFailure(ioExc);
                        streamHandler.onFailure(streamId, ioExc);
                    }
                }

                @Override
                public void onFailure(String streamId, Throwable cause) throws IOException {
                    callback.onFailure(new IOException("Destination failed while reading stream", cause));
                    streamHandler.onFailure(streamId, cause);
                }

                @Override
                public String getID() {
                    return streamHandler.getID();
                }
            };
            if (req.bodyByteCount > 0) {
                StreamInterceptor<RequestMessage> interceptor = new StreamInterceptor<>(
                        this, wrappedCallback.getID(), req.bodyByteCount, wrappedCallback);
                frameDecoder.setInterceptor(interceptor);
            } else {
                wrappedCallback.onComplete(wrappedCallback.getID());
            }
        } catch (Exception e) {
            logger.error("Error while invoking RpcHandler#receive() on RPC id " + req.requestId, e);
            respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
            // We choose to totally fail the channel, rather than trying to recover as we do in other
            // cases.  We don't know how many bytes of the stream the client has already sent for the
            // stream, it's not worth trying to recover.
            channel.pipeline().fireExceptionCaught(e);
        } finally {
            req.meta.release();
        }
    }

    private void processStreamRequest(StreamRequest req) {
        if (logger.isTraceEnabled()) {
            logger.trace("Received req from {} to fetch stream {}", getRemoteAddress(channel),
                    req.streamId);
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
            buf = streamManager.openStream(req.streamId);
        } catch (Exception e) {
            logger.error(String.format(
                    "Error opening stream %s for request from %s", req.streamId, getRemoteAddress(channel)), e);
            respond(new StreamFailure(req.streamId, Throwables.getStackTraceAsString(e)));
            return;
        }

        if (buf != null) {
            streamManager.streamBeingSent(req.streamId);
            respond(new StreamResponse(req.streamId, buf.size(), buf)).addListener(future -> {
                streamManager.streamSent(req.streamId);
            });
        } else {
            // org.apache.spark.repl.ExecutorClassLoader.STREAM_NOT_FOUND_REGEX should also be updated
            // when the following error message is changed.
            respond(new StreamFailure(req.streamId, String.format(
                    "Stream '%s' was not found.", req.streamId)));
        }
    }

    private void processOneWayMessage(OneWayMessage req) {
        try {
            rpcHandler.receive(reverseClient, req.body().nioByteBuffer());
        } catch (Exception e) {
            logger.error("Error while invoking RpcHandler#receive() for one-way message.", e);
        } finally {
            req.body().release();
        }
    }

    private void processRpcRequest(final RpcRequest req) {
        try {
            rpcHandler.receive(reverseClient, req.body().nioByteBuffer(), new RpcResponseCallback() {
                @Override
                public void onSuccess(ByteBuffer response) {
                    respond(new RpcResponse(req.requestId, new NioManagedBuffer(response)));
                }

                @Override
                public void onFailure(Throwable e) {
                    respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
                }
            });
        } catch (Exception e) {
            logger.error("Error while invoking RpcHandler#receive() on RPC id " + req.requestId, e);
            respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
        } finally {
            req.body().release();
        }
    }

    @Override
    public void channelActive() {
        rpcHandler.channelActive(reverseClient);
    }

    @Override
    public void exceptionCaught(Throwable cause) {
        rpcHandler.exceptionCaught(cause, reverseClient);
    }

    @Override
    public void channelInactive() {
        if (streamManager != null) {
            try {
                streamManager.connectionTerminated(channel);
            } catch (RuntimeException e) {
                logger.error("StreamManager connectionTerminated() callback failed.", e);
            }
        }
        rpcHandler.channelInactive(reverseClient);
    }

    private ChannelFuture respond(Encodable result) {
        SocketAddress remoteAddress = channel.remoteAddress();
        return channel.writeAndFlush(result).addListener(future -> {
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
