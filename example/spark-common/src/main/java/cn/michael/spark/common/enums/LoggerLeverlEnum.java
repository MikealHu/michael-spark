package cn.michael.spark.common.enums;

/**
 * author: Michael Hu
 * email: hufenggang2019@gmail.com
 * date: 2019/12/20 14:14
 */
public enum LoggerLeverlEnum {
    INFO("INFO"),
    DEBUG("DEBUG"),
    WARN("WARN");

    private final String value;

    LoggerLeverlEnum(String value) {
        this.value = value;
    }

    public String value() {
        return this.value;
    }
}
