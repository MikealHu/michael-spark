package cn.michael.spark.internal

import org.apache.log4j.spi.{Filter, LoggingEvent}

/**
 * Created by hufenggang on 2020/1/10.
 */
private class SparkShellLoggingFilter extends Filter {

    /**
     * If sparkShellThresholdLevel is not defined, this filter is a no-op.
     * If log level of event is not equal to root level, the event is allowed. Otherwise,
     * the decision is made based on whether the log came from root or some custom configuration
     * @param loggingEvent
     * @return decision for accept/deny log event
     */
    def decide(loggingEvent: LoggingEvent): Int = {
        if (Logging.sparkShellThresholdLevel == null) {
            Filter.NEUTRAL
        } else if (loggingEvent.getLevel.isGreaterOrEqual(Logging.sparkShellThresholdLevel)) {
            Filter.NEUTRAL
        } else {
            var logger = loggingEvent.getLogger()
            while (logger.getParent() != null) {
                if (logger.getLevel != null || logger.getAllAppenders.hasMoreElements) {
                    return Filter.NEUTRAL
                }
                logger = logger.getParent()
            }
            Filter.DENY
        }
    }
}
