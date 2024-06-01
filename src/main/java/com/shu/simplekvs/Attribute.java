package com.shu.simplekvs;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public abstract class Attribute {
	String item;
	int length;
	
	
	public Attribute(String item) {
		this.item = item;
		this.length = item.length();
	}
	
	protected byte[] toByte() {
		byte[] byteLength = ByteBuffer.allocate(4).putInt(this.length).array();
		byte[] byteItem = this.item.getBytes(StandardCharsets.UTF_8);
		return ArrayUtil.combineByteArray(byteLength, byteItem);
	}
}
