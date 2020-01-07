package cn.michael.spark.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * 可编码为ByteBuf对象的接口。多个Encodable对象存储在一个预先分配的ByteBuf中，
 * 因此Encodables必须提供编码长度。
 */
public interface Encodable {

    /**
     * 该项目编码的字节数个数
     */
    int encodedLength();

    /**
     * 利用写入提供的字节缓存来序列化该项目
     */
    void encode(ByteBuf buf);
}
