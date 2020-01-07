package cn.michael.spark.network.protocol;

import cn.michael.spark.network.buffer.ManagedBuffer;
import com.google.common.base.Objects;

/**
 * Abstract class for messages which optionally contain a body kept in a separate buffer.
 */
public abstract class AbstractMessage implements Message {
    private final ManagedBuffer body;
    private final boolean isBodyInFrame;

    public AbstractMessage() {
        this(null, false);
    }

    protected AbstractMessage(ManagedBuffer body, boolean isBodyInFrame) {
        this.body = body;
        this.isBodyInFrame = isBodyInFrame;
    }

    @Override
    public ManagedBuffer body() {
        return body;
    }

    @Override
    public boolean isBodyInFrame() {
        return isBodyInFrame;
    }

    protected boolean equals(AbstractMessage other) {
        return isBodyInFrame == other.isBodyInFrame && Objects.equal(body, other.body);
    }
}
