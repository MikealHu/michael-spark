package cn.michael.spark.network.client;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * author: hufenggang
 * email: hufenggang2019@gmail.com
 * date: 2019/12/25 10:08
 *
 * 回调流数据。
 * 流数据将在到达时提供{@link #onData(String, ByteBuffer)}方法；
 * 在接到所有流数据时，将调用{@link #onComplete(String)}方法。
 * <p>
 * 网络库保证单个线程一次会调用这些方法，但是不同的线程可能会进行不同的调用。
 */
public interface StreamCallback {
    /** 在收到流数据时调用 */
    void onData(String streamId, ByteBuffer buf) throws IOException;

    /** 收到流中所有数据时调用 */
    void onComplete(String streamId) throws IOException;

    /** 从流中读取数据有报错时调用 */
    void onFailure(String streamId, Throwable cause) throws IOException;
}
