package com.dp.miniDBMS.cm;

import java.io.IOException;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;

import com.dp.miniDBMS.sm.smm.CheckPoint;
import com.dp.miniDBMS.sm.smm.LogManager;
import com.dp.miniDBMS.sm.smm.MemoryDicTBSManager;

/**
 *
 * @날짜 : Aug 2, 2020
 * @작성자 : 박선희
 * @클래스설명 : 실서버 배포용
 *
 */
public class TestDaemon implements Daemon, Runnable {

	private String status = "";
	
	private Thread dbServer = null;
	private Thread ckptThread = null;
	
	private ClientHandler clients = null;
	
	public void loadTBSDic() {
		System.out.println("table space dictionary를 읽는 중 입니다. ");
		MemoryDicTBSManager startMdtbm = MemoryDicTBSManager.getInstance();
		startMdtbm.loadDicTBS();
	}
	public void flushTBS() {
		System.out.println("메모리에 있는 정보를 저장 중 입니다... ");
		
		MemoryDicTBSManager mtbsm = MemoryDicTBSManager.getInstance();
		mtbsm.flush();
		
		System.out.println("log file을 저장 중 입니다... ");
		CheckPoint.getInstance().isTerminate = true;
	}
	
	@Override
	public void init(DaemonContext context) throws DaemonInitException, Exception {
		System.out.println("init...");
		String[] args = context.getArguments();
		if (args != null) {
			for (String arg : args) {
				System.out.println(arg);
			}
		}
		status = "INITED";
		
		loadTBSDic();
		System.out.println("DB 서버 설정 완료");

		System.out.println("init OK.");
		System.out.println();
	}

	@Override
	public void start() {
		System.out.println("status: " + status);
		System.out.println("start...");
		status = "STARTED";
		
		try {
			this.clients = new ClientHandler();
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.clients.startClientHandler(); //client start
		
		this.dbServer = new Thread(this.clients);
		this.dbServer.start(); //client handler run 
		
		/*** check point thread *****/
		
		System.out.println("checkpoint thread를 실행합니다.");
		CheckPoint ct = CheckPoint.getInstance();
		ckptThread = new Thread(ct);
		this.ckptThread.start();

		System.out.println("start OK.");
		System.out.println();
	}

	@Override
	public void stop() {
		System.out.println("status: " + status);
		System.out.println("stop...");
		status = "STOPED";

		this.clients.stopClientHandler(); //client handler 종료
		flushTBS();
		while(this.ckptThread.isAlive());
			
		try {
			this.dbServer.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("DB가 종료되었습니다. "); //server daemon thread 종료
		
		System.out.println("stop OK.");
		System.out.println();
	}

	@Override
	public void destroy() {
		System.out.println("status: " + status);
		System.out.println("destroy...");
		status = "DESTROYED";
		System.out.println("destroy OK.");
		System.out.println();
	}

	@Override
	public void run() {
		while (true) {
			// try {
			// Thread.sleep(1000);
			// } catch (InterruptedException e) {
			// e.printStackTrace();
			// }
		}
	}

}
