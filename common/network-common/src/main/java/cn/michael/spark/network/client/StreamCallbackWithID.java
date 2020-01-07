package cn.michael.spark.network.client;

/**
 * author: hufenggang
 * email: hufenggang2019@gmail.com
 * date: 2019/12/25 10:08
 */
public interface StreamCallbackWithID extends StreamCallback {
    String getID();
}
