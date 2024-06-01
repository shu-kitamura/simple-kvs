package com.shu.simplekvs;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Memtable {
	private int sizeLimit;
	private List<Row> rowList;
	private Set<String> keySet;
	
	public Memtable(int sizeLimit) {
		this.sizeLimit = sizeLimit;
		this.rowList = new ArrayList<Row>();
		this.keySet  = new HashSet<String>();
	}
	
	
	protected void put(Row row) {
		this.rowList.add(row);
		this.keySet.add(row.getKey());
		
		if (rowList.size() >= this.sizeLimit) {
			this.flush();
		}
	}
	
	protected String get(String key) {
		if (this.keySet.contains(key)) {
			// 含む処理
			String value = "";
			return value;
		} else {
			return null;
		}
	}
	
	private void flush() {
		System.out.println("flush !!!");
	}
}
