package cn.michael.spark.network.protocol;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

/**
 * author: Michael Hu
 * email: hufenggang2019@gmail.com
 * date: 2019/12/20 11:06
 * 为简单类提供一些规范的编码器
 */
public class Encoders {

    public static class Strings {
        public static int encodedLength(String s) {
            return 4 + s.getBytes(StandardCharsets.UTF_8).length;
        }

        public static void encode(ByteBuf buf, String s) {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            buf.writeInt(bytes.length);
            buf.writeBytes(bytes);
        }

        public static String decode(ByteBuf buf) {
            int length = buf.readInt();
            byte[] bytes = new byte[length];
            buf.readBytes(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    public static class ByteArrays {
        public static int encodedLength(byte[] arr) {
            return 4 + arr.length;
        }

        public static void encode(ByteBuf buf, byte[] arr) {
            buf.writeInt(arr.length);
            buf.writeBytes(arr);
        }

        public static byte[] decode(ByteBuf buf) {
            int length = buf.readInt();
            byte[] bytes = new byte[length];
            buf.readBytes(bytes);
            return bytes;
        }
    }

    public static class StringArrays {
        public static int encodedLength(String[] strings) {
            int totalLength = 4;
            for (String s : strings) {
                totalLength += Strings.encodedLength(s);
            }
            return totalLength;
        }

        public static void encode(ByteBuf buf, String[] strings) {
            buf.writeInt(strings.length);
            for (String s : strings) {
                Strings.encode(buf, s);
            }
        }

        public static String[] decode(ByteBuf buf) {
            int numStrings = buf.readInt();
            String[] strings = new String[numStrings];
            for (int i = 0; i < strings.length; i ++) {
                strings[i] = Strings.decode(buf);
            }
            return strings;
        }
    }

    public static class IntArrays {
        public static int encodedLength(int[] ints) {
            return 4 + 4 * ints.length;
        }

        public static void encode(ByteBuf buf, int[] ints) {
            buf.writeInt(ints.length);
            for (int i : ints) {
                buf.writeInt(i);
            }
        }

        public static int[] decode(ByteBuf buf) {
            int numInts = buf.readInt();
            int[] ints = new int[numInts];
            for (int i = 0; i < ints.length; i ++) {
                ints[i] = buf.readInt();
            }
            return ints;
        }
    }
}
