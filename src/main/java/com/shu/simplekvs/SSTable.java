package com.shu.simplekvs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SSTable {
    private Path path;
    private Path indexPath;
    private Map<String, Integer> index;

    public SSTable(String path, Map<String, String> memtable) throws IOException{
        this.path = Paths.get(path);
        Long timestamp = System.currentTimeMillis();
        
        boolean isDir = Files.isDirectory(this.path);
        boolean exists = Files.exists(this.path);
        
        if (isDir && exists) {
            Path file = Paths.get(String.format("sstab_%d.dat", timestamp));
            this.path = this.path.resolve(file);
        } else {
        	if (exists) {
        		// pathがファイルの時の例外
        		throw new FileAlreadyExistsException(String.format("%s is already exist.", this.path));
        	} else if (isDir) {
        		// pathが存在しない時の例外
        		throw new NoSuchFileException(String.format("%s is not found.", this.path));
        	} else {
        		// その他(ファイルが存在しないなど)の例外
        		throw new FileNotFoundException(String.format("%s is not found.", this.path));
        	}
        }

        this.index = new HashMap<>();
        this.dumpKV(memtable);
        this.indexPath = this.dumpIndex(this.index);
    }

    public SSTable(String path) throws FileNotFoundException, IOException{
        this.path = Paths.get(path);
        
        if (!Files.exists(this.path) || Files.isDirectory(this.path)) {
            throw new FileNotFoundException(String.format("%s is not found.", path));
        }

        this.indexPath = Paths.get(path + ".index");
        this.index = loadIndex(this.indexPath.toString());
    }
    
    public SSTable(Path path) throws IOException{
    	this(path.toString());
    }

    protected String get(String key) throws IOException{
    	int position = this.index.get(key);
    	String value = "";
    	try (
            FileInputStream fis = new FileInputStream(this.path.toString());
    		BufferedInputStream bis = new BufferedInputStream(fis);
        ) {
    		String[] kv = IOUtils.loadKV(bis, position);
    		value = kv[1];
    	}
    	return value;
    }
    
    protected boolean containsKey(String key) {
    	return this.index.containsKey(key);
    }
    
    protected Set<String> getKeySet(){
    	return this.index.keySet();
    }
    
    protected String getPath() {
    	return this.path.toString();
    }
    
    protected void delete() throws IOException{
    	Files.delete(this.indexPath);
    	Files.delete(this.path);
    }
    /*
     * 以下、privateメソッド
     */
    private void dumpKV(Map<String, String> memtable) throws IOException{
        try (
            FileOutputStream fos = new FileOutputStream(this.path.toString());
            BufferedOutputStream bos = new BufferedOutputStream(fos)
            ) {
        	    // indexにpositionをPutし、Key,Valueをファイルに書き込む
                int position = 0;
                for (Map.Entry<String, String> kv : memtable.entrySet()){
                    String key = kv.getKey();
                    String value = kv.getValue();
                    this.index.put(key, position);
                    // key, valueの長さ + 8(それぞれの長さを4バイトで表すための4*2) をpositionに加える
                    // ＝次のpositionを求める
                    position += key.length() + value.length() + 8;
                    IOUtils.dumpKV(bos, key, value);
                }
            }
    }
    
    private Path dumpIndex(Map<String, Integer> index)  throws IOException{
    	String path = this.path.toString() + ".index";
    	try (
            FileOutputStream fos = new FileOutputStream(path);
    		BufferedOutputStream bos = new BufferedOutputStream(fos)
    	) {
    		for (Map.Entry<String, Integer> kp : this.index.entrySet()) {
    			String key = kp.getKey();
    			int position = kp.getValue();
    			IOUtils.dumpIndex(bos, key, position);
    		}
    	}
    	return Paths.get(path);
    }
    
    private Map<String, Integer> loadIndex(String path) throws IOException{
    	try(
    		FileInputStream fis = new FileInputStream(path);
    		BufferedInputStream bis = new BufferedInputStream(fis)
    	) {
    		Map<String, Integer> index = IOUtils.loadIndex(bis);
    		return index;
    	}
    }
}
