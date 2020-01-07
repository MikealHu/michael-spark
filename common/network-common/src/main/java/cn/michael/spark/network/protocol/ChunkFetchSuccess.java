package cn.michael.spark.network.protocol;

import cn.michael.spark.network.buffer.ManagedBuffer;
import cn.michael.spark.network.buffer.NettyManagedBuffer;
import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

/**
 * author: hufenggang
 * email: hufenggang2019@gmail.com
 * date: 2019/12/24 13:44
 */
public final class ChunkFetchSuccess extends AbstractResponseMessage {
    public final StreamChunkId streamChunkId;

    public ChunkFetchSuccess(StreamChunkId streamChunkId, ManagedBuffer buffer) {
        super(buffer, true);
        this.streamChunkId = streamChunkId;
    }

    @Override
    public Type type() {
        return Type.ChunkFetchSuccess;
    }

    @Override
    public int encodedLength() {
        return streamChunkId.encodedLength();
    }

    @Override
    public void encode(ByteBuf buf) {
        streamChunkId.encode(buf);
    }

    @Override
    public ResponseMessage createFailureResponse(String error) {
        return new ChunkFetchFailure(streamChunkId, error);
    }

    public static ChunkFetchSuccess decode(ByteBuf buf) {
        StreamChunkId streamChunkId = StreamChunkId.decode(buf);
        buf.retain();
        NettyManagedBuffer managedBuf = new NettyManagedBuffer(buf.duplicate());
        return new ChunkFetchSuccess(streamChunkId, managedBuf);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(streamChunkId, body());
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ChunkFetchSuccess) {
            ChunkFetchSuccess o = (ChunkFetchSuccess) other;
            return streamChunkId.equals(o.streamChunkId) && super.equals(o);
        }
        return false;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("streamChunkId", streamChunkId)
                .add("buffer", body())
                .toString();
    }
}
