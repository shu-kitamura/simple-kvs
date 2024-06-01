package com.shu.simplekvs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

public class skvs {
	private static final String HELP = "get, deleteの時\n\tjava skvs <get or delete> <key>\nputの時\n\tjava skvs put <key> <value>";
	
	public static void main(String[] args) {
		if (!skvs.checkArgs(args)) {
			System.out.println(String.format("\n[ERROR] 引数が間違っています\nUsage :\n%s", skvs.HELP));
			return;
		}
		
		byte[][] byteArgs = skvs.convArgsToBytes(args);
		
		byte[] bytes = ArrayUtil.combineArray(byteArgs);

		// クライアントソケットを生成
		try (Socket socket = new Socket("localhost", 10000);
			 OutputStream os = socket.getOutputStream();
			 BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))
		) {
			os.write(bytes);
			os.flush();
			System.out.println(reader.readLine());
		} catch (IOException e){
			e.printStackTrace();
		}
	}
	
	private static boolean checkArgs(String args[]) {
		if (args.length == 0) {
			return false;
		}
		
		boolean result;
		switch (args[0]) {
		case "get":
			result = args.length == 2 ? true : false;
			break;
		case "delete":
			result = args.length == 2 ? true : false;
			break;
		case "put":
			result = args.length == 3 ? true : false;
			break;
		default:
			result = false;
			break;
		};
		return result;
	}
	
	private static byte[][] convArgsToBytes(String[] args) {
		byte [][] byteArgs = new byte[args.length*2-1][];
		int methodCode = skvs.getMethodCode(args[0]);

		int index = 0;
		byteArgs[index] = ByteBuffer.allocate(4).putInt(methodCode).array();

		for (String strKV : ArrayUtil.slice(args, 1, args.length)) {
			byte[][] bytes = IOUtils.getByteStrAndLength(strKV);
	        byte[] byteStr = bytes[0];
	        byte[] byteLenStr = bytes[1];
			byteArgs[++index] = byteLenStr;
			byteArgs[++index] = byteStr;
		}
		return byteArgs;
	}
	
	private static int getMethodCode(String method) {
		int methodCode;
		switch(method) {
			case "get":
				methodCode = 0;
				break;
			case "put":
				methodCode = 1;
				break;
			case "delete":
				methodCode = 2;
				break;
			default:
				methodCode = 99;
		};
		return methodCode;
	}
}
