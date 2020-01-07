package cn.michael.spark.network.buffer;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * author: Michael Hu
 * email: hufenggang2019@gmail.com
 * date: 2019/12/20 10:48
 *
 * 基于netty的ByteBuf做了上层的封装
 */
public class NettyManagedBuffer extends ManagedBuffer {
    private final ByteBuf buf;

    public NettyManagedBuffer(ByteBuf buf) {
        this.buf = buf;
    }

    @Override
    public long size() {
        return buf.readableBytes();
    }

    @Override
    public ByteBuffer nioByteBuffer() throws IOException {
        return buf.nioBuffer();
    }

    @Override
    public InputStream createInputStream() throws IOException {
        return new ByteBufInputStream(buf);
    }

    @Override
    public ManagedBuffer retain() {
        buf.retain();
        return this;
    }

    @Override
    public ManagedBuffer release() {
        buf.release();
        return this;
    }

    @Override
    public Object convertToNetty() throws IOException {
        return Objects.toStringHelper(this)
                .add("buf", buf)
                .toString();
    }
}
