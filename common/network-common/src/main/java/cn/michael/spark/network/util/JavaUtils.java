package cn.michael.spark.network.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Locale;

/**
 * author: Michael Hu
 * email: hufenggang2019@gmail.com
 * date: 2019/12/23 11:14
 */
public class JavaUtils {
    private static final Logger logger = LoggerFactory.getLogger(JavaUtils.class);

    /**
     * 在此定义驱动程序内存的默认值，因为该值在整个代码库中都被引用，
     * 并且几乎所有文件都已使用Utils.scala
     */
    public static final long DEFAULT_DRIVER_MEM_MB = 1024;

    public static void closeQuietly(Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException e) {
            logger.error("IOException should not have been thrown.", e);
        }
    }

    public static long byteStringAs(String str) {
        return 0L;
    }

    private static long byteStringAs(String str, ByteUnit aByte) {
        String lower = str.toLowerCase(Locale.ROOT).trim();
        return 0;
    }

    public static long byteStringAsBytes(String str) {
        return byteStringAs(str, ByteUnit.BYTE);
    }


}
