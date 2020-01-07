package cn.michael.spark.network.protocol;

import cn.michael.spark.network.buffer.ManagedBuffer;
import io.netty.buffer.ByteBuf;

/**
 * author: hufenggang
 * email: hufenggang2019@gmail.com
 * date: 2019/12/24 11:22
 */
public final class StreamResponse extends AbstractResponseMessage {
    public final String streamId;
    public final long byteCount;

    public StreamResponse(String streamId, long byteCount, ManagedBuffer buffer) {
        super(buffer, false);
        this.streamId = streamId;
        this.byteCount = byteCount;
    }

    @Override
    public Type type() {
        return Type.StreamResponse;
    }

    @Override
    public int encodedLength() {
        return 8 + Encoders.Strings.encodedLength(streamId);
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, streamId);
        buf.writeLong(byteCount);
    }

    public static StreamResponse decode(ByteBuf buf) {
        String streamId = Encoders.Strings.decode(buf);
        long byteCount = buf.readLong();
        return new StreamResponse(streamId, byteCount, null);
    }

    @Override
    public ResponseMessage createFailureResponse(String error) {
        return null;
    }
}
