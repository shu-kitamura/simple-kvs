package com.shu.simplekvs;

import java.nio.ByteBuffer;

public class Row {
	private final Key key;
	private final Value value;
	private final Boolean isDeleted;
	
	public Row(String key, String value, boolean isDeleted) {
		this.key = new Key(key);
		this.value = new Value(value);
		this.isDeleted = isDeleted;
	}
	
	protected String getKey() {
		return this.key.item;
	}
	
	protected String getValue() {
		return this.value.item;
	}
	
	protected boolean isDeleted() {
		return this.isDeleted;
	}
	
	private byte[] toByte() {
		byte[][] bytes = new byte[3][];
		bytes[0] = this.key.toByte();
		bytes[1] = this.value.toByte();
		int boolHashCode = this.isDeleted.hashCode();
		bytes[2] = ByteBuffer.allocate(4).putInt(boolHashCode).array();
		return ArrayUtil.combineArray(bytes);
	}
		
	private static Row fromByte(byte[] rowByte) {
		String[] items = new String[2];

		int offset = 0;
		for (int i=0; i<2; i++) {
			// Key,Valueの長さを取得
			byte[] bytes = ArrayUtil.slice(rowByte, offset, offset+4);
			int itemLen = ByteBuffer.wrap(bytes).getInt();

			// offsetに読み込んだbyte数(4byte)を足す
			offset += 4;

			// Key,Valueの値を取得
			bytes = ArrayUtil.slice(rowByte, offset, offset+itemLen);
			items[i] = new String(bytes);
			
			// offsetに読み込んだbyte数(4byte)を足す
			offset += itemLen;
		}
		
		byte[] bytes = ArrayUtil.slice(rowByte, offset, offset+4);
		int boolHashCode = ByteBuffer.wrap(bytes).getInt();
		boolean isDelete = boolHashCode==1231; // 1231 = TrueのHashCode

		return new Row(items[0], items[1], isDelete);

	}
	
	public static void main(String args[]) {
		Key k = new Key("key1");
		Value v = new Value("value1");
		Row r = new Row("key1","value1",false);
		Row r2 = new Row("key2","value2",true);

		byte[] rowByte = r.toByte();
		Row r3 = Row.fromByte(rowByte);
		System.out.println(r3.getKey() + " " + r3.getValue());
		


//		byte[] b1 = ArrayUtil.slice(k.toByte(), 0, 4);
//		int l = ByteBuffer.wrap(b1).getInt();
//		System.out.println(l);
//		byte[] b2 = ArrayUtil.slice(k.toByte(), 4, 4+l);
//		System.out.println(new String(b2));
	}
}
