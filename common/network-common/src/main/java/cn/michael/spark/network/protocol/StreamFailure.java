package cn.michael.spark.network.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

/**
 * author: hufenggang
 * email: hufenggang2019@gmail.com
 * date: 2019/12/24 11:27
 */
public class StreamFailure extends AbstractMessage implements ResponseMessage {
    public final String streamId;
    public final String error;

    public StreamFailure(String streamId, String error) {
        this.streamId = streamId;
        this.error = error;
    }

    @Override
    public Type type() {
        return Type.StreamFailure;
    }

    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(streamId) +
                Encoders.Strings.encodedLength(error);
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, streamId);
        Encoders.Strings.encode(buf, error);
    }

    public static StreamFailure decode(ByteBuf buf) {
        String streamId = Encoders.Strings.decode(buf);
        String error = Encoders.Strings.decode(buf);
        return new StreamFailure(streamId, error);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(streamId, error);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof StreamFailure) {
            StreamFailure o = (StreamFailure) other;
            return streamId.equals(o.streamId) && error.equals(o.error);
        }
        return false;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("streamId", streamId)
                .add("error", error)
                .toString();
    }
}
