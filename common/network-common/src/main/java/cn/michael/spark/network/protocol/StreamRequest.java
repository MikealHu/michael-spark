package cn.michael.spark.network.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

/**
 * author: Michael Hu
 * email: hufenggang2019@gmail.com
 * date: 2019/12/24 10:48
 */
public final class StreamRequest extends AbstractMessage implements RequestMessage {
    public final String streamId;

    public StreamRequest(String streamId) {
        this.streamId = streamId;
    }

    @Override
    public Type type() {
        return Type.StreamRequest;
    }

    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(streamId);
    }

    // 编码
    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, streamId);
    }

    // 解码
    public static StreamRequest decode(ByteBuf buf) {
        String streamId = Encoders.Strings.decode(buf);
        return new StreamRequest(streamId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(streamId);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof StreamRequest) {
            StreamRequest o = (StreamRequest) other;
            return streamId.equals(o.streamId);
        }
        return false;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("streamId", streamId)
                .toString();
    }
}
