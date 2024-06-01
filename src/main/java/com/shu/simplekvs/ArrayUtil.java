package com.shu.simplekvs;

import java.util.Arrays;

public class ArrayUtil {
	protected static byte[] combineArray(byte[][] byteArrays) {
		byte[] array = byteArrays[0];
		for(int i = 1; i < byteArrays.length; i++) {
			array = ArrayUtil.combineByteArray(array, byteArrays[i]);
		}
		return array;
	}

    protected static byte[] combineByteArray(byte[] byteArray1, byte[] byteArray2) {
        // 各配列の長さを取得
        int length1 = byteArray1.length;
        int length2 = byteArray2.length;

        byte[] combinedArray = new byte[length1 + length2];

        // 順に結合
        System.arraycopy(byteArray1, 0, combinedArray, 0, length1);
        System.arraycopy(byteArray2, 0, combinedArray, length1, length2);

        return combinedArray;
    }
    
    // byte配列のスライス用関数
    public static byte[] slice(byte[] arr, int stIndx, int enIndx) {
        byte[] sclicedArr = Arrays.copyOfRange(arr, stIndx, enIndx);
        return sclicedArr;
    }
    
    // string配列のスライス用関数
    public static String[] slice(String[] arr, int stIndx, int enIndx) {
        String[] sclicedArr = Arrays.copyOfRange(arr, stIndx, enIndx);
        return sclicedArr;
    }
}