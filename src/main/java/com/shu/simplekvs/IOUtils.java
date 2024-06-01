package com.shu.simplekvs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class IOUtils {
    // KeyとValueをファイルに書き込む関数
    public static void dumpKV(BufferedOutputStream bos, String key, String value) throws IOException {
        // keyをbyte配列にエンコードし、長さを取得
        byte[][] KeyAndLen = IOUtils.getByteStrAndLength(key);
        // valueをbyte配列にエンコードし、長さを取得
        byte[][] valueAndLen = IOUtils.getByteStrAndLength(value);

    	byte[][] bytes = {KeyAndLen[1], KeyAndLen[0], valueAndLen[1], valueAndLen[0]};
        // byte配列の結合
        byte[] writeBytes = ArrayUtil.combineArray(bytes);

        bos.write(writeBytes);
    }

    public static String[] loadKV(BufferedInputStream bis, int position) throws IOException {
        String[] kvPair = new String[2]; 
        bis.skip(position);
        for (int i = 0 ; i < 2; i++) {
            byte[] bytes = new byte[4];
            bis.read(bytes, 0, bytes.length);
            int length = ByteBuffer.wrap(bytes).getInt();
            byte[] byteStr = new byte[length];
            bis.read(byteStr, 0, byteStr.length);
            kvPair[i] = new String(byteStr);
        }
        return kvPair;
    }

    public static void dumpIndex(BufferedOutputStream bos, String key, int position) throws IOException {
        byte[][] KeyAndLen = IOUtils.getByteStrAndLength(key);
        byte[] posBytes = ByteBuffer.allocate(4).putInt(position).array();        
        byte[][] bytes = {KeyAndLen[1], KeyAndLen[0], posBytes};
        byte[] writeBytes = ArrayUtil.combineArray(bytes);
        bos.write(writeBytes);
    }

    public static Map<String, Integer> loadIndex(BufferedInputStream bis) throws IOException {
        Map<String, Integer> index = new HashMap<>();
        byte[] bytes = new byte[4];
        int read;
        while ((read = bis.read(bytes, 0, bytes.length)) != -1) {
            int length = ByteBuffer.wrap(bytes).getInt();
            byte[] byteKey = new byte[length];
            bis.read(byteKey, 0, byteKey.length);

            bis.read(bytes, 0, bytes.length);
            String key = new String(byteKey);
            int position = ByteBuffer.wrap(bytes).getInt();
            index.put(key, position);
        }
        return index;
    }

    protected static byte[][] getByteStrAndLength(String str) {
        byte[][] bytes = new byte[2][];
        bytes[0] = str.getBytes(StandardCharsets.UTF_8);
        int strLength = bytes[0].length;
        bytes[1] = ByteBuffer.allocate(4).putInt(strLength).array();
        return bytes;
    }
}