package com.dp.miniDBMS.cm;

import java.io.IOException;

import com.dp.miniDBMS.sm.smm.CheckPoint;
import com.dp.miniDBMS.sm.smm.MemoryDicTBSManager;
/**
  * 
  * @날짜 : Aug 1, 2020 
  * @작성자 : 박선희 
  * @클래스설명 : DB server
  * 
*/
public class Server{
	private Thread dbServer = null;
	private Thread ckptThread = null;
	private ClientHandler clients = null;
	
	private boolean runFlag = false;
	
	public void loadTBSDic() {
		System.out.println("table space dictionary를 읽는 중 입니다. ");
		MemoryDicTBSManager startMdtbm = MemoryDicTBSManager.getInstance();
		startMdtbm.loadDicTBS();
	}
	public void flushTBS() {
		System.out.println("메모리에 있는 정보를 저장 중 입니다... ");
		
		MemoryDicTBSManager mdtbsm = MemoryDicTBSManager.getInstance();
		mdtbsm.flush();
		
		System.out.println("log buffer를 저장 중 입니다... ");
		CheckPoint.getInstance().isTerminate = true;
	}
	public void start() {
		this.runFlag = true;
		
		loadTBSDic();
		
		System.out.println("checkpoint thread를 실행합니다.");
		CheckPoint ct = CheckPoint.getInstance();
		ckptThread = new Thread(ct);
		
		try {
			this.clients = new ClientHandler();
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.clients.startClientHandler();
		
		this.dbServer = new Thread(this.clients);
		this.dbServer.start(); //client handler run 
		this.ckptThread.start();
		
		return;
	}
	public void stop() {
		this.runFlag = false;
		this.clients.stopClientHandler();
		flushTBS();
		
		System.out.println("DB가 종료되었습니다. ");
		return;
	}
}
