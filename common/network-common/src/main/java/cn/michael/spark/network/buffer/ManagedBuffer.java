package cn.michael.spark.network.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * author: Michael Hu
 * email: hufenggang2019@gmail.com
 * date: 2019/12/13 11:24
 *
 * 该接口以字节的方式提供了一个数据视图，该实现应指定如何提供数据：
 *
 * - {@link FileSegmentManagedBuffer}: data backed by part of a file
 * - {@link NioManagedBuffer}: data backed by a NIO ByteBuffer
 * - {@link NettyManagedBuffer}: data backed by a Netty ByteBuf
 *
 * 可能在JVM垃圾收集器之外管理具体的缓冲区实现。
 * 例如，如果使用{@link NettyManagedBuffer}，则对缓冲区进行引用计数。
 * 在这种情况下，如果要将缓冲区传递给其他线程，则应调用保留/释放。
 */
public abstract class ManagedBuffer {

    /**
     * Number of bytes of the data. If this buffer will decrypt for all of the views into the data,
     * this is the size of the decrypted data.
     */
    public abstract long size();

    /**
     * Exposes this buffer's data as an NIO ByteBuffer. Changing the position and limit of the
     * returned ByteBuffer should not affect the content of this buffer.
     */
    // TODO: Deprecate this, usage may require expensive memory mapping or allocation.
    public abstract ByteBuffer nioByteBuffer() throws IOException;

    /**
     * Exposes this buffer's data as an InputStream. The underlying implementation does not
     * necessarily check for the length of bytes read, so the caller is responsible for making sure
     * it does not go over the limit.
     */
    public abstract InputStream createInputStream() throws IOException;

    /**
     * Increment the reference count by one if applicable.
     */
    public abstract ManagedBuffer retain();

    /**
     * If applicable, decrement the reference count by one and deallocates the buffer if the
     * reference count reaches zero.
     */
    public abstract ManagedBuffer release();

    /**
     * Convert the buffer into an Netty object, used to write the data out. The return value is either
     * a {@link io.netty.buffer.ByteBuf} or a {@link io.netty.channel.FileRegion}.
     * <p>
     * If this method returns a ByteBuf, then that buffer's reference count will be incremented and
     * the caller will be responsible for releasing this new reference.
     */
    public abstract Object convertToNetty() throws IOException;
}
