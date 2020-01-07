package cn.michael.spark.network.client;

import java.nio.ByteBuffer;

/**
 * author: hufenggang
 * email: hufenggang2019@gmail.com
 * date: 2019/12/25 10:07
 */
public interface RpcResponseCallback {

    void onSuccess(ByteBuffer response);

    void onFailure(Throwable e);
}
