package cn.michael.spark.network.protocol;

import cn.michael.spark.network.buffer.ManagedBuffer;
import cn.michael.spark.network.buffer.NettyManagedBuffer;
import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

/**
 * author: Michael Hu
 * email: hufenggang2019@gmail.com
 * date: 2019/12/20 10:44
 *
 * RPC类型的消息是Spark用来进行RPC操作时发送的消息，主要用于发送控制类的消息。
 *
 * RPC消息包括：
 * - {@link RpcRequest}: Rpc请求
 * - {@link RpcResponse}: Rpc响应
 * - {@link RpcFailure}: Rpc响应
 *
 * 每个RpcRequest消息都要求服务端回传一个Rpc响应（RpcResponse/RpcFailure），
 * 在发送RpcRequst消息时，会在客户端注册一个回调函数，并绑定到消息的唯一标识符requestId上。
 * 当服务端回传RPC响应时，客户端会根据回传消息中的requestId，
 * 找到注册的回调函数，然后调用回调函数来执行服务端响应后的逻辑。
 */
public final class RpcRequest extends AbstractMessage implements RequestMessage {
    /** 用于连接Rpc请求及其响应 */
    public final long requestId;

    public RpcRequest(long requestId, ManagedBuffer message) {
        super(message, true);
        this.requestId = requestId;
    }

    @Override
    public Type type() {
        return Type.RpcRequest;
    }

    @Override
    public int encodedLength() {
        return 8 + 4;
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeLong(requestId);
        buf.writeInt((int) body().size());
    }

    public static RpcRequest decode(ByteBuf buf) {
        long requestId = buf.readLong();
        // See comment in encodedLength().
        buf.readInt();
        return new RpcRequest(requestId, new NettyManagedBuffer(buf.retain()));
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof RpcRequest) {
            RpcRequest rpcRequest = (RpcRequest) other;
            return requestId == rpcRequest.requestId && super.equals(rpcRequest);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(requestId, body());
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("requestId", requestId)
                .add("body", body())
                .toString();
    }
}
