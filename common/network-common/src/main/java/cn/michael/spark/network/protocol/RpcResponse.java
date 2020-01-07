package cn.michael.spark.network.protocol;

import cn.michael.spark.network.buffer.ManagedBuffer;
import cn.michael.spark.network.buffer.NettyManagedBuffer;
import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

/**
 * author: Michael Hu
 * email: hufenggang2019@gmail.com
 * date: 2019/12/23 10:55
 */
public class RpcResponse extends AbstractResponseMessage {
    public final long requestId;

    public RpcResponse(long requestId, ManagedBuffer message) {
        super(message, true);
        this.requestId = requestId;
    }

    @Override
    public Type type() {
        return Type.RpcResponse;
    }

    @Override
    public int encodedLength() {
        return 8 + 4;
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeLong(requestId);
        // See comment in encodedLength().
        buf.writeInt((int) body().size());
    }

    @Override
    public ResponseMessage createFailureResponse(String error) {
        return new RpcFailure(requestId, error);
    }

    public static RpcResponse decode(ByteBuf buf) {
        long requestId = buf.readLong();
        // See comment in encodedLength().
        buf.readInt();
        return new RpcResponse(requestId, new NettyManagedBuffer(buf.retain()));
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(requestId, body());
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof RpcResponse) {
            RpcResponse o = (RpcResponse) other;
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
