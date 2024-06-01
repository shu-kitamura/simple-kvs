package com.shu.simplekvs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class SimpleKVS {
    private static final int PORT = 10000;
    private static final int MEMTABLE_LIMIT_DEFAULT = 1024;
    private static final String LOGDIR_DEFAULT = "log";
    private static final String LOGFILE_NAME = "SimpleKVS.log";
    private static final String DATADIR_DEFAULT = "data";
    private static final List<String> METHODLIST = new ArrayList<>(Arrays.asList("put","get","delete")); 
    private Logger logger = Logger.getLogger("SampleLogging");
    
    protected static final int SUCESS = 0;
    protected static final int FAILED = 1;

    private Path dataDir;
    private Path logDir;
    private Map<String, String> memtable;
    private int memtableLimit;
    private List<SSTable> sstableList;
    private WAL wal;

    public SimpleKVS(String dataDir, String logDir, int memtableLimit) {
    	this.logDir = Paths.get(logDir);
    	
    	if (!Files.exists(this.logDir)) {
        	try {
        		Files.createDirectories(this.logDir);
        	} catch (IOException e) {
        		e.printStackTrace();
        	}
        }
    	
    	try {
    		LogManager.getLogManager().reset();
    		// 出力ファイルを指定
    		FileHandler fh = new FileHandler(logDir +File.separator+ LOGFILE_NAME, true);
    		// フォーマット指定
    		fh.setFormatter(new SimpleFormatter());
    		this.logger.addHandler(fh);
    	} catch (IOException e) {
    		e.printStackTrace();
    	}
    	
    	this.logger.log(Level.INFO, "Launch SimpleKVS.");
    	

        this.dataDir = Paths.get(dataDir);
    	this.logger.log(Level.INFO, String.format("Set \"%s\" as the data directory.", this.dataDir));
        
        // ディレクトリが存在しない場合、作成する
    	this.makeDir(this.dataDir);
        
        this.memtable = new TreeMap<String, String>();
        this.memtableLimit = memtableLimit;
        this.logger.log(Level.INFO, String.format("Set \"%d\" as number of the limit of memtable.", this.memtableLimit));
        
        // SSTable読み込み処理
        this.sstableList = new ArrayList<SSTable>();
        this.loadSSTables(dataDir);
        
        // Compaction
        this.majorCompaction(this.sstableList);
        
        // WAL読込み処理
        this.logger.log(Level.INFO, String.format("Load WAL."));
        try {
        	this.wal = new WAL(dataDir);
        	this.memtable = this.wal.recovery();
        } catch (IOException e) {
        	this.logger.log(Level.WARNING, "The following exception occurred in the process loading WAL.", e);
        }
        this.logger.log(Level.INFO, String.format("Loading WAL is complete."));
        
        this.logger.log(Level.INFO, "Finish to launch SimpleKVS");
    }

    public SimpleKVS(String dataDir, String logDir) {
    	this(dataDir, logDir, SimpleKVS.MEMTABLE_LIMIT_DEFAULT);
    }
    
    public SimpleKVS(String dataDir) {
        this(dataDir, SimpleKVS.LOGDIR_DEFAULT, SimpleKVS.MEMTABLE_LIMIT_DEFAULT);
    }

    public SimpleKVS() {
        this(SimpleKVS.DATADIR_DEFAULT, SimpleKVS.LOGDIR_DEFAULT, SimpleKVS.MEMTABLE_LIMIT_DEFAULT);
    }
    
    public String get(String key) {
    	String value;
    	this.logger.log(Level.INFO, String.format("Operation is get. Key is \"%s\"", key));
    	if (this.memtable.containsKey(key)) {
            value = this.memtable.get(key);
        } else {
        	value = this.getFromSSTable(key);
        }
    	// 削除されているかチェックしてreturn
    	return this.isDeleted(value) ? null : value;
    }

    public void put(String key, String value) {
    	this.logger.log(Level.INFO, String.format("Operation is put. Key is \"%s\", Value is \"%s\"", key, value));
    	
    	this.writeWAL(key, value);
    	this.memtable.put(key, value);
    	this.logger.log(Level.INFO, String.format("Key \"%s\" and Value \"%s\" are written to Memtable.", key, value));

        if (this.memtable.size() >= this.memtableLimit) {
        	// Memtableのサイズが上限を超えた処理
        	this.logger.log(Level.INFO, "Number of rows in memtable have reached the limit, so flush memtable to SSTable");
        	SSTable sstable = this.flush(this.dataDir.toString(), this.memtable);
        	this.sstableList.add(sstable);
            this.memtable = new TreeMap<String, String>();
            try{
            	this.logger.log(Level.INFO, "Clean up WAL.");
            	this.wal.cleanUp();
            } catch (IOException e){
            	this.logger.log(Level.WARNING, "The following exception occurred in the process to clean up WAL.", e);
            }
        }
    }

    public void delete(String key) {
    	this.logger.log(Level.INFO, String.format("Operation is delete. Key is \"%s\"", key));
        this.writeWAL(key, "__tombstone__");
        this.memtable.put(key, "__tombstone__");
    }

    /*
     以下、privateのメソッド
     */ 
    private boolean isDeleted(String value) {
        return value.equals("__tombstone__");
    }
    
    
    private void makeDir(Path dirPath) {
    	if (!Files.exists(dirPath)) {
        	this.logger.log(Level.INFO, String.format("\"%s\" isn't exist, so create a directory.", dirPath));
        	try {
        		Files.createDirectories(dirPath);
        	} catch (IOException e) {
        		this.logger.log(Level.WARNING, "The following exception occurred in the process of creating directory.", e);
        	}
        }
    }
    
    private void writeWAL(String key, String value) {
    	try {
    		this.wal.put(key, value);
        	this.logger.log(Level.INFO, String.format("Key \"%s\" and Value \"%s\" are written to WAL.", key, value));
    	} catch (IOException e) {
    		this.logger.log(Level.WARNING, "The following exception occurred in the process writing to WAL.", e);
    	}
    }
    
    private void loadSSTables(String dataDir) {
    	this.logger.log(Level.INFO, "Load SSTables.");
    	//ロードしたSSTableは時系列の古い順でリストに格納される
        File[] files = new File(dataDir).listFiles();
        
        for (File file : files) {
        	Path path = file.toPath();
        	String fileName = path.getFileName().toString();
        	if (fileName.startsWith("sstab") && fileName.endsWith(".dat")) {
        		this.logger.log(Level.INFO, String.format("Load SSTable \"%s\"", path));
        		try {
        			this.sstableList.add(new SSTable(path));
        		} catch (IOException e) {
        			this.logger.log(Level.WARNING, String.format("The following exception occurred in the process loading SSTable \"%s\".", path), e);
        		}
        		this.logger.log(Level.INFO, String.format("Loading SSTable \"%s\" is complete.", path));
        	}
        }
        this.logger.log(Level.INFO, "Loading SSTables is complete.");
    }
    
    private String getFromSSTable(String key) {
    	String value = "";
    	try {
    		// SSTableのリストには古い順で格納されるので、逆順でループする
    		for (int i=(this.sstableList.size()-1); i>=0; i--) {
    			SSTable sstable = this.sstableList.get(i);
    			if (sstable.containsKey(key)) {
            		value = sstable.get(key);
            		break;
    			}
        	}
    	} catch (IOException e) {
    		this.logger.log(Level.WARNING, "The following exception occurred in the process getting value in SSTable.", e);
    	}
    	return value;
    }
    
    private SSTable flush(String path, Map<String, String> memtable) {
    	SSTable sstable = null;
    	try {
    		sstable = new SSTable(path, memtable);
    	} catch (IOException e) {
    		this.logger.log(Level.WARNING, "The following exception occurred in the process flush memtable to SSTable.", e);
    	}
    	return sstable;
    }
    
    private void majorCompaction(List<SSTable> sstableList) {
    	this.logger.log(Level.INFO, "Major compaction start.");
    	
    	// SSTableの数が２以下の場合、処理せず終了する
    	if (sstableList.size() <= 2) {
    		this.logger.log(Level.WARNING, "Compaction isn't execute because the number of SSTables was less than two.");
    		return;
    	}
    	
    	Map<String, String> mergedMemtable = this.mergeSSTables(sstableList);
    	
    	SSTable newSSTable = this.flush(this.dataDir.toString(), mergedMemtable);
    	
    	this.deleteSSTable(this.sstableList);
    	this.sstableList.clear();
    	this.sstableList.add(newSSTable);
    	this.logger.log(Level.INFO, "Major compaction finish.");
    }
    
    private Map<String, String> mergeSSTables(List<SSTable> sstableList) {
    	this.logger.log(Level.INFO, "Merge exists SSTables.");
    	Map<String, String> mergedMemtable = new TreeMap<String, String>();

    	for (SSTable sstab : sstableList) {
    		for (String key : sstab.getKeySet()) {
    			try {
    				String value = sstab.get(key);
    				if (this.isDeleted(value)) {
    					mergedMemtable.remove(key);
    					continue;
    				}
    				mergedMemtable.put(key, value);
    			} catch (IOException e) {
    				this.logger.log(Level.WARNING, "The following exception occurred in the process getting value in SSTable.", e);
    			}
    		}
    	}
    	return mergedMemtable;
    }
    private void deleteSSTable(List<SSTable> sstableList) {
    	this.logger.log(Level.INFO, "Delete merged SSTables");
    	for (SSTable sstable : sstableList) {
    		try {
    			sstable.delete();
    		} catch (IOException e) {
    			this.logger.log(Level.WARNING, "The following exception occurred in the process deleting SSTables.", e);
    		}
    	}
    }
        
    private void run() {
    	try (ServerSocket server = new ServerSocket(SimpleKVS.PORT)) {
    		this.logger.log(Level.INFO, String.format("Listening for incoming connections on port %d.", SimpleKVS.PORT));
    		while(true) {
    			Socket socket = server.accept();
    			InputStream is = socket.getInputStream();
    			this.logger.log(Level.INFO, String.format("New connection established from %s.", socket.getRemoteSocketAddress()));

    			byte [] buf = new byte[1024];
    			is.read(buf);

    			Map<String, String> req = this.getRequest(buf);

    			PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
    			if (!SimpleKVS.METHODLIST.contains(req.get("method"))){
    				writer.println("invalid method");
    				continue;
    			}
    			
    			String execRes = this.execOperation(req);
    			
    			writer.println(execRes);
    		}
    	} catch (IOException e) {
    		this.logger.log(Level.WARNING, "The following exception occurred in the process incoming connection.", e);
    	}
    }
    
    private Map<String, String> getRequest(byte[] buffer) {
    	Map<String, String> request = new HashMap<>();
 
    	byte[] byteCode = ArrayUtil.slice(buffer, 0, 4);
		int methodCode = ByteBuffer.wrap(byteCode).getInt();
		
		String method = this.getMethod(methodCode);
		request.put("method", method);
		
		String key;
		String value;
		switch(methodCode) {
			case 0:
				// getの時
				key = this.getStrFromBytes(buffer, 4);
				request.put("key", key);
				break;
			case 1:
				// putの時の処理
				key = this.getStrFromBytes(buffer, 4);
				request.put("key", key);
				value = this.getStrFromBytes(buffer, 8 + key.length()); // MethodCode, Key長のByte配列の長さの合計である4*2 = 8を足す 
				request.put("value", value);
				break;
			case 2:
				// deleteの時
				key = this.getStrFromBytes(buffer, 4);
				request.put("key", key);
				break;
			default:
				this.logger.log(Level.WARNING, String.format("Method \"%s\"is invalid.", method));
				break;
		}
		return request;
    }

    private String getStrFromBytes(byte[] bytes, int startIndex) {
    	int endIndex = startIndex + 4; // Key、Value長を4byteの配列に変換しているため、+4としている
    	
    	byte[] lenBytes = ArrayUtil.slice(bytes, startIndex, endIndex);
    	int length = ByteBuffer.wrap(lenBytes).getInt();
    	
    	startIndex += 4; // StartIndexに取得した4byte分足す
    	endIndex = startIndex + length;
    	
    	byte[] strBytes = ArrayUtil.slice(bytes, startIndex, endIndex);
    	return new String(strBytes);
    }
    
    private String getMethod(int methodCode) {
    	// methodCode(int)からmethod(String)に変換する
    	String method;
    	switch (methodCode) {
    	case 0:
    		method = "get";
    		break;
    	case 1:
    		method = "put";
    		break;
    	case 2:
    		method = "delete";
    		break;
    	default:
    		method = null;
    		break;
    	}
    	return method;
    }
    
    private String execOperation(Map<String, String> request) {
    	String result;
    	String key;
    	key = request.get("key");
    	switch(request.get("method")) {
    		case "get":
    			result = this.get(key);
    			break;
			case "put":
				String value = request.get("value");
			    this.put(key, value);
			    result = "success put"; // TODO : 失敗時の処理や返却内容の検討
			    break;
			case "delete":
				this.delete(key);
				result = "success delete";
				break;
			default:
			    result =  "Invalid method";
			    break;
    	};
    	return result;
    }
    
    public static void main(String[] args) {
    	if (args.length > 1) {
    		System.out.println("Invalid argments.");
    		System.exit(1);
    	}

    	//String dataDir = args.length == 1 ? args[0] : "data"; // dataを保存するディレクトリ(デフォルトはdata)
    	SimpleKVS kvs = new SimpleKVS();
    	kvs.run();
    }
}
