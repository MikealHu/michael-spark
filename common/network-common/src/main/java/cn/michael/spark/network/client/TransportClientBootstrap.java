package cn.michael.spark.network.client;

import io.netty.channel.Channel;

/**
 * author: hufenggang
 * email: hufenggang2019@gmail.com
 * date: 2019/12/25 10:08
 *
 * ::客户端引导程序::
 * 这个保证在每个连接一次基础上初始交换信息（例如：SASL认证）
 *
 * 由于连接（和TransportClients）被尽可能多的重用，因此执行昂贵的引导操作通常是合理的，
 * 因为他们通常和JVM共享生命周期
 */
public interface TransportClientBootstrap {
    /** 执行引导操作，执行失败时引发异常 */
    void doBootstrap(TransportClient client, Channel channel) throws RuntimeException;
}
