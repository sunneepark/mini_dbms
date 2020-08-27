package com.dp.miniDBMS;

/**
  *
  * @날짜 : Aug 12, 2020 
  * @작성자 : 박선희 
  * @클래스설명 : client thread(runnable) 실행 (로컬 테스트용)
  *
 */
public class ClientDo {

	public static void main(String[] args) {
		
		Thread clientThread = null;

		Client client = new Client();
		
		clientThread = new Thread(client);
		clientThread.start();
	}

}
