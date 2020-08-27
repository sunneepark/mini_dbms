package com.dp.miniDBMS.cm;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
  *
  * @날짜 : Aug 1, 2020 
  * @작성자 : 박선희 
  * @클래스설명 : client thread 관리
  *
 */

public class ClientHandler implements Runnable {
	private Object lockObj = null;
	private static final int PORT = 1228;
	
	private boolean flag = false;
	
	private ServerSocket listener = null;
	
	private ExecutorService pool = null;
	
	public ClientHandler() throws IOException {
		this.listener = new ServerSocket(PORT);
		this.pool = Executors.newFixedThreadPool(4); //쓰레드풀 생성
		System.out.println("[SERVER] Start listening for client...");
	}

	public void startClientHandler() {
		this.flag = true;
	}
	public void stopClientHandler() {
		this.flag = false;
		try {
			if (!pool.awaitTermination(3, TimeUnit.SECONDS)) {
				pool.shutdownNow();
		    }
		} catch (InterruptedException ie) {
		  // (Re-)Cancel if current thread also interrupted
		  pool.shutdownNow();
		  Thread.currentThread().interrupt();
		}
		
		try {
			listener.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void run() {
		while(this.flag) {
			Socket client = null;
			try {
				this.listener.setSoTimeout(2000); 
				client = listener.accept();
				
				if(client != null) {
					System.out.println("[SERVER] Connected to client!");
					try {
						//session 추가
						SessionManager sm = SessionManager.getInstance();
						
						Session session = new Session();
						session.setCurrentClient(client);
						sm.setSesion(session);
						
						//client server 생성
						ClientServer clientServer = new ClientServer(session); 
						pool.execute(clientServer);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			} catch (IOException e) {
			
			}
		}
	}
}
