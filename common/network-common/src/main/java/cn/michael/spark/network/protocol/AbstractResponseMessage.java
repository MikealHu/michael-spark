package cn.michael.spark.network.protocol;

import cn.michael.spark.network.buffer.ManagedBuffer;

/**
 * author: Michael Hu
 * email: hufenggang2019@gmail.com
 * date: 2019/12/23 10:58
 */
public abstract class AbstractResponseMessage extends AbstractMessage implements ResponseMessage {

    protected AbstractResponseMessage(ManagedBuffer body, boolean isBodyInFrame) {
        super(body, isBodyInFrame);
    }

    public abstract ResponseMessage createFailureResponse(String error);
}
