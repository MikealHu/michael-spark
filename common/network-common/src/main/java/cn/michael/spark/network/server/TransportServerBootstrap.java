package cn.michael.spark.network.server;

import io.netty.channel.Channel;

/**
 * author: hufenggang
 * email: hufenggang2019@gmail.com
 * date: 2019/12/26 11:55
 */
public interface TransportServerBootstrap {

    RpcHandler doBootstrap(Channel channel, RpcHandler rpcHandler);
}
