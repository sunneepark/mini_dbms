package com.dp.miniDBMS.cm;

import java.io.*;
import java.net.Socket;

import com.dp.miniDBMS.sm.smo.DicDatabase;

/**
 * 
  *
  * @날짜 : Aug 14, 2020 
  * @작성자 : 박선희 
  * @클래스설명 : user thread 의 세션
  *
 */
public class Session {
	
	private DicDatabase currentDB;
	private Socket currentClient;
	private PrintWriter streamToClient;
	private BufferedReader streamFromClient;
	
	private int sessionID; //세션 고유 id

	public String getCurrentDatabase() {
		if(currentDB == null)
			return null;
		else
			return currentDB.getDatabaseName();
	}

	public void setCurrentDatabase(DicDatabase currentDatabase) {
		this.currentDB = currentDatabase;
	}

	public Socket getCurrentClient() {
		return currentClient;
	}

	public void setCurrentClient(Socket currentClient) {
		this.currentClient = currentClient;
		this.sessionID = this.currentClient.hashCode();
		
		InputStream inStream = null;
		OutputStream outStream = null;
		BufferedReader bfReader = null;
		PrintWriter ptWriter = null;
		try {
			bfReader = new BufferedReader(new InputStreamReader(this.currentClient.getInputStream()));
			ptWriter = new PrintWriter(this.currentClient.getOutputStream());
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			this.streamFromClient = bfReader;
			this.streamToClient = ptWriter;
		}
	}

	public PrintWriter getStreamToClient() {
		return streamToClient;
	}

	public BufferedReader getStreamFromClient() {
		return streamFromClient;
	}
	public int getSessionId() {
		return this.sessionID;
	}

}
