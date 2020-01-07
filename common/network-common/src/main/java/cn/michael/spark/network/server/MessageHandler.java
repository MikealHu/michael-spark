package cn.michael.spark.network.server;

import cn.michael.spark.network.protocol.Message;

/**
 * author: hufenggang
 * email: hufenggang2019@gmail.com
 * date: 2019/12/25 9:57
 *
 * 处理来自Netty的请求或响应，
 * MessageHandler实例与单个Netty Channel关联（尽管它可能在同一Channel上有多个客户端）
 */
public abstract class MessageHandler<T extends Message> {
    /** 处理单个消息的接受 */
    public abstract void handle(T message) throws Exception;

    /** 当此MessageHandler所在的通道处于活动状态时调用。 */
    public abstract void channelActive();

    /** 当Channel捕捉到异常时调用 */
    public abstract void exceptionCaught(Throwable cause);

    /** 当此MessageHandler所在的通道处于非活动状态时调用。 */
    public abstract void channelInactive();
}
