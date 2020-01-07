package cn.michael.spark.network.client;

import cn.michael.spark.network.buffer.ManagedBuffer;

/**
 * author: hufenggang
 * email: hufenggang2019@gmail.com
 * date: 2019/12/25 10:06
 *
 * 单个块结果的回调。
 * 对于单个流，保证回调由同一线程以与请求块相同的顺序来调用。
 *
 * 注意：如果发生一般流故障，则所有未完成的块请求可能会失败。
 */
public interface ChunkReceivedCallback {

    /**
     * 在收到特定块时调用。
     *
     * 给定的缓冲区最初的引用计数为1，但一旦此调用返回，将被释放。
     * 因此，您必须在返回之前保留（）缓冲区或复制其内容。
     */
    void onSuccess(int chunkIndex, ManagedBuffer buffer);

    /**
     * 调用失败以获取特定的块。
     * 注意：实际上可能由于未能获取此流中的先前块而调用此方法。
     *
     * 收到故障后，该流可能有效也可能无效。 客户端不应假定流的服务器端已关闭。
     */
    void onFailure(int chunkIndex, Throwable e);
}
