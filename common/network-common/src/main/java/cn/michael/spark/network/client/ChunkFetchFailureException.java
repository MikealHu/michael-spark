package cn.michael.spark.network.client;

/**
 * author: hufenggang
 * email: hufenggang2019@gmail.com
 * date: 2019/12/25 11:25
 */
public class ChunkFetchFailureException extends RuntimeException {
    public ChunkFetchFailureException(String errorMsg, Throwable cause) {
        super(errorMsg, cause);
    }

    public ChunkFetchFailureException(String errorMsg) {
        super(errorMsg);
    }
}
