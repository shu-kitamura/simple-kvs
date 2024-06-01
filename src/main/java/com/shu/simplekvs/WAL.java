package com.shu.simplekvs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Map;
import java.util.TreeMap;

public class WAL {
    private static final String FILENAME = "wal.dat";
    private String path;

    public WAL(String dataDir) throws IOException{
        this.path = dataDir + File.separator + WAL.FILENAME;
        File file = new File(this.path);
        if (!file.exists()) {
        	file.createNewFile();
        }
    }

    protected void put(String key, String value) throws IOException{
    	try (
            FileOutputStream fos = new FileOutputStream(this.path,true);
            BufferedOutputStream bos = new BufferedOutputStream(fos);
        ) {
        	FileChannel ch = fos.getChannel();
    		FileLock lock = null;
    		try {
        		lock = ch.tryLock();
        		IOUtils.dumpKV(bos, key, value);
    		} finally {
    			if(lock != null) lock.release();
    		}
    	}
    }

    protected Map<String, String> recovery() throws IOException{
    	Map<String, String> memtable = new TreeMap<String, String>();
    	try(
        	FileInputStream fis = new FileInputStream(this.path);
        	BufferedInputStream bis = new BufferedInputStream(fis)
        ) {
    		while (bis.available() > 1) {
    			String[] kv = IOUtils.loadKV(bis, 0);
    			String key = kv[0];
    			String value = kv[1];
    			memtable.put(key, value);
    		}
    		return memtable;
    	}
    }

    protected void cleanUp() throws IOException{
    	FileOutputStream fos = new FileOutputStream(this.path, false);
    	fos.close();
    }
}
