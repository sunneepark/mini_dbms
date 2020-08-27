package com.dp.miniDBMS;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.security.SecureRandom;

/**
 * 
 *
 * @날짜 : Aug 12, 2020
 * @작성자 : 박선희
 * @클래스설명 : client thread
 *
 */
public class Client implements Runnable {
	
	private static final String SERVER_IP = "127.0.0.1";
	private static final int SERVER_PORT = 1228;

	private Socket socket = null;
	private BufferedReader input = null;
	private PrintWriter out = null;

	private BufferedReader keyboard = null; //엔터만 경계로 인식 
	private PrintWriter keyboardout = null;

	private void showWelcome() {
		System.out.println("================================================================================");
		System.out.println("****                      Welcome to  DBMS                                  ****");
		System.out.println("================================================================================\n");

	}
	
	private void readLine(BufferedReader reader) {
		StringBuilder sb = new StringBuilder();
		String response = "";
		int lineNum;
		try {
			lineNum = Integer.parseInt(reader.readLine());
			while (lineNum-- > 0) {
				String line = reader.readLine();
				sb.append(line).append("\n");
			}
		} catch (NumberFormatException | IOException e) {
			e.printStackTrace();
		}
		keyboardout.println(sb.toString());
		keyboardout.flush();
		// System.out.print(sb.toString());
	}

	/** 랜덤 문자열을 생성한다 **/
	public String generate(String DATA, int length) {

		SecureRandom random = new SecureRandom();
		if (length < 1)
			throw new IllegalArgumentException("length must be a positive number.");
		StringBuilder sb = new StringBuilder(length);
		for (int i = 0; i < length; i++) {
			sb.append(DATA.charAt(random.nextInt(DATA.length())));
		}
		return sb.toString();
	}

	
	private int queryRun;
	public void insertSQL() throws IOException {
		String ENGLISH_LOWER = "abcdefghijklmnopqrstuvwxyz";
		String ENGLISH_UPPER = ENGLISH_LOWER.toUpperCase();
		String NUMBER = "0123456789";

		/** 랜덤을 생성할 대상 문자열 **/
		String DATA_FOR_RANDOM_STRING = ENGLISH_LOWER + ENGLISH_UPPER + NUMBER;

		/** 랜덤 문자열 길이 **/
		int random_string_length = 10;

		queryRun = 0;
		for (int i = 0; i < 150000; i++) {
			double dValue = Math.random();
			int c1 = (int) (dValue * 50000) + 1;
			double dValue2 = Math.random();
			int c2 = (int) (dValue2 * 10000) + 1;
			String s = "insert into sunny.sno values (" + c1 + "," + c2 + ",\'"
					+ generate(DATA_FOR_RANDOM_STRING, random_string_length) + "\');";

			out.println(s);
			out.flush();
			//readLine(input);
			readlineWhenBufferReady();
		}

		while(queryRun < 150000) {
			readlineWhenBufferReady();
		}
			
	}
	
	private void readlineWhenBufferReady() throws IOException {
		while(input.ready()) {
		     String line = input.readLine();
		     try {
		    	Integer.parseInt(line);
		    	continue;
		     } catch(NumberFormatException e) {
		    	 
		     }
		     queryRun++;
			 System.out.println(line);
		}
	}

	// 소켓 연결
	private void start() throws IOException {
		socket = new Socket(SERVER_IP, SERVER_PORT);

		input = new BufferedReader(new InputStreamReader(socket.getInputStream())); // 서버로 부터 온 input
		out = new PrintWriter(socket.getOutputStream(), true); // 서버로 쓸 스트림

		keyboard = new BufferedReader(new InputStreamReader(System.in)); // 클라이언트 콘솔 스트림
		keyboardout = new PrintWriter(new OutputStreamWriter(System.out));
	}

	@Override
	public void run() {
		try {
			this.start();
			showWelcome();

			while (socket != null) {
				System.out.print("sql> ");
				String command;

				command = keyboard.readLine();

				if (command.equals("load data")) {
					// 파일 객체 생성
					this.loadData();
				} else if (command.equals("quit;")) {
					out.println(command);
					break;
				} else {
					out.println(command);
					out.flush();
					readLine(input);
				}
			}

			System.out.println("bye!");
			socket.close();
		} catch (IOException e) {
			e.getMessage();
			System.out.println("[error] socket broken!");
		}
		return;
	}

	private void loadData() throws IOException {
		try {
			File file = new File("/root/data1.txt");
			// 입력 스트림 생성
			FileReader filereader = new FileReader(file);
			// 입력 버퍼 생성
			BufferedReader bufReader = new BufferedReader(filereader);
			
			queryRun = 0;
			for (int i = 0; i < 150000; i++) {
				String s = bufReader.readLine();
				out.println(s);
				out.flush();
				//readLine(input);
				readlineWhenBufferReady();
			}
			bufReader.close();
		} catch (FileNotFoundException e) {
			System.out.println(e);
		} catch (IOException e) {
			System.out.println(e);
		}
		
		while(queryRun < 150000) {
			readlineWhenBufferReady();
		}
	}
}
