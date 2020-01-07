package cn.michael.spark.network.protocol;

import cn.michael.spark.network.buffer.ManagedBuffer;
import cn.michael.spark.network.buffer.NettyManagedBuffer;
import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * author: hufenggang
 * email: hufenggang2019@gmail.com
 * date: 2019/12/26 11:48
 */
public class UploadStream extends AbstractMessage implements RequestMessage {
    /**
     * Used to link an RPC request with its response.
     */
    public final long requestId;
    public final ManagedBuffer meta;
    public final long bodyByteCount;

    public UploadStream(long requestId, ManagedBuffer meta, ManagedBuffer body) {
        super(body, false); // body is *not* included in the frame
        this.requestId = requestId;
        this.meta = meta;
        bodyByteCount = body.size();
    }

    // this version is called when decoding the bytes on the receiving end.  The body is handled
    // separately.
    private UploadStream(long requestId, ManagedBuffer meta, long bodyByteCount) {
        super(null, false);
        this.requestId = requestId;
        this.meta = meta;
        this.bodyByteCount = bodyByteCount;
    }

    @Override
    public Message.Type type() {
        return Type.UploadStream;
    }

    @Override
    public int encodedLength() {
        // the requestId, meta size, meta and bodyByteCount (body is not included)
        return 8 + 4 + ((int) meta.size()) + 8;
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeLong(requestId);
        try {
            ByteBuffer metaBuf = meta.nioByteBuffer();
            buf.writeInt(metaBuf.remaining());
            buf.writeBytes(metaBuf);
        } catch (IOException io) {
            throw new RuntimeException(io);
        }
        buf.writeLong(bodyByteCount);
    }

    public static UploadStream decode(ByteBuf buf) {
        long requestId = buf.readLong();
        int metaSize = buf.readInt();
        ManagedBuffer meta = new NettyManagedBuffer(buf.readRetainedSlice(metaSize));
        long bodyByteCount = buf.readLong();
        // This is called by the frame decoder, so the data is still null.  We need a StreamInterceptor
        // to read the data.
        return new UploadStream(requestId, meta, bodyByteCount);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(requestId);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof UploadStream) {
            UploadStream o = (UploadStream) other;
            return requestId == o.requestId && super.equals(o);
        }
        return false;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("requestId", requestId)
                .add("body", body())
                .toString();
    }
}
