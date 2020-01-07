package cn.michael.spark.network.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

/**
 * author: hufenggang
 * email: hufenggang2019@gmail.com
 * date: 2019/12/24 13:38
 */
public final class ChunkFetchRequest extends AbstractMessage implements RequestMessage {
    public final StreamChunkId streamChunkId;

    public ChunkFetchRequest(StreamChunkId streamChunkId) {
        this.streamChunkId = streamChunkId;
    }

    @Override
    public Type type() {
        return Type.ChunkFetchRequest;
    }

    @Override
    public int encodedLength() {
        return streamChunkId.encodedLength();
    }

    @Override
    public void encode(ByteBuf buf) {
        streamChunkId.encode(buf);
    }

    public static ChunkFetchRequest decode(ByteBuf buf) {
        return new ChunkFetchRequest(StreamChunkId.decode(buf));
    }

    @Override
    public int hashCode() {
        return streamChunkId.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ChunkFetchRequest) {
            ChunkFetchRequest o = (ChunkFetchRequest) other;
            return streamChunkId.equals(o.streamChunkId);
        }
        return false;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("streamChunkId", streamChunkId)
                .toString();
    }
}
