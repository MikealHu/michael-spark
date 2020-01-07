package cn.michael.spark.network.client;

import cn.michael.spark.network.protocol.Message;
import cn.michael.spark.network.server.MessageHandler;
import cn.michael.spark.network.util.TransportFrameDecoder;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

/**
 * author: hufenggang
 * email: hufenggang2019@gmail.com
 * date: 2019/12/25 11:26
 */
public class StreamInterceptor<T extends Message> implements TransportFrameDecoder.Interceptor {
    private final MessageHandler<T> handler;
    private final String streamId;
    private final long byteCount;
    private final StreamCallback callback;
    private long bytesRead;

    public StreamInterceptor(
            MessageHandler<T> handler,
            String streamId,
            long byteCount,
            StreamCallback callback) {
        this.handler = handler;
        this.streamId = streamId;
        this.byteCount = byteCount;
        this.callback = callback;
        this.bytesRead = 0;
    }

    @Override
    public boolean handle(ByteBuf buf) throws Exception {
        int toRead = (int) Math.min(buf.readableBytes(), byteCount - bytesRead);
        ByteBuffer nioBuffer = buf.readSlice(toRead).nioBuffer();

        int available = nioBuffer.remaining();
        callback.onData(streamId, nioBuffer);
        bytesRead += available;
        if (bytesRead > byteCount) {
            RuntimeException re = new IllegalStateException(String.format(
                    "Read too many bytes? Expected %d, but read %d.", byteCount, bytesRead));
            callback.onFailure(streamId, re);
            deactivateStream();
            throw re;
        } else if (bytesRead == byteCount) {
            deactivateStream();
            callback.onComplete(streamId);
        }

        return bytesRead != byteCount;
    }

    @Override
    public void exceptionCaught(Throwable cause) throws Exception {
        deactivateStream();
        callback.onFailure(streamId, cause);
    }

    @Override
    public void channelInactive() throws Exception {
        deactivateStream();
        callback.onFailure(streamId, new ClosedChannelException());
    }

    private void deactivateStream() {
        if (handler instanceof TransportResponseHandler) {
            // we only have to do this for TransportResponseHandler as it exposes numOutstandingFetches
            // (there is no extra cleanup that needs to happen)
            ((TransportResponseHandler) handler).deactivateStream();
        }
    }
}
