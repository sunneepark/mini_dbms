package com.dp.miniDBMS.cm;

/**
 *
 * @날짜 : Aug 1, 2020
 * @작성자 : 박선희
 * @클래스설명 : 로컬 테스트용
 *
 */
public class ServerDo {

	private static Server server = null;

	public static void main(String[] args) {
		server = new Server();
		server.start();

		boolean stop=false;
		while (!stop) {
			try {
				Thread.sleep(50000);
				stop=true;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		server.stop();
	}
}

