package com.dp.miniDBMS.sm.smm;

/**
  *
  * @날짜 : Aug 21, 2020 
  * @작성자 : 박선희 
  * @클래스설명 : checkpoint thread 
  *
 */
public class CheckPoint extends Thread{

	private static int checkpointSCN = 0;
	
	public boolean isRunning = false;
	public boolean isTerminate = false;
	private static CheckPoint instance = new CheckPoint();

	private CheckPoint() { isRunning = true;}

	public static CheckPoint getInstance() {
		return instance;
	}

	//checkpoint running
	public void run() {
		while(isRunning) {
			try {
				//checkpoint 시점이 commit 시점보다 늦을때 
				if(checkpointSCN < CommitManager.getInstance().viewSCN) { 
					System.out.println(checkpointSCN+" checkpoint is running!");
					LogManager lm = LogManager.getInstance();
					lm.doCheckpoint();
					System.out.println(++checkpointSCN+ " checkpoint is flushed!");
				}
				else if(isTerminate) {
					LogManager lm = LogManager.getInstance();
					lm.doCheckpoint();
					isRunning = false;
				}
				sleep(1000); //1초로 
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		System.out.println("checkpoint thread 가 종료되었습니다.");
	}
}
