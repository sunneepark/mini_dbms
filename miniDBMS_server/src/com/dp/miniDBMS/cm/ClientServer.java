package com.dp.miniDBMS.cm;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import com.dp.miniDBMS.qp.qpe.Command;
import com.dp.miniDBMS.qp.qpp.ParseException;
import com.dp.miniDBMS.qp.qpp.SQLParser;
import com.dp.miniDBMS.qp.qpp.SemanticException;
import com.dp.miniDBMS.qp.qpp.TokenMgrError;
import com.dp.miniDBMS.sm.smm.AgerManager;
import com.dp.miniDBMS.sm.smm.CommitManager;

/**
 * 
 * @날짜 : Aug 1, 2020
 * @작성자 : 박선희
 * @클래스설명 : client 각각의 server thread
 * 
 */

public class ClientServer implements Runnable {
	private Object lockObj = null;
	
	private Socket client = null;	
	private Session session = null;
	private int sessionId =0;
	private BufferedReader in = null;
	private PrintWriter out = null;

	private boolean flag = false;
	private String conectionName;

	public ClientServer(Session session) throws IOException {
		this.flag = true;
		this.session = session;

		this.client = this.session.getCurrentClient();
		this.conectionName = this.client.getInetAddress() + ":"
				+ this.client.getPort() + "[" + this.client.hashCode() + "]";
		
		System.out.println(this.conectionName);
		
		this.sessionId = this.session.getSessionId();
		this.in = new BufferedReader(new InputStreamReader(this.client.getInputStream()));
		
		this.out = new PrintWriter(this.client.getOutputStream(), true);
	}
	
	public String doQuery(String request) {	
		SQLParser parser = new SQLParser(System.in);
		Command command = null;
		
		InputStream commandStream = new ByteArrayInputStream(
				request.getBytes());
		
		//parsing 과정 시작
		try {
			parser.ReInit(commandStream);
			
			long start = System.currentTimeMillis();
			command = parser.Command();
			
			command.setTrxId(TransactionManager.getInstance().makeTrx(session.getSessionId()));
			command.setSessionId(sessionId);
			command.setDb(session.getCurrentDatabase());
			
			String result = command.validate();
			long end = System.currentTimeMillis();
	
			//실행 계획 보여 줄때 
			if(command.isExplain) {
				return result + "\t ("+ ((end-start)/1000.0) +" sec)\n\n"
						+TransactionManager.getInstance().getExplain(sessionId);
			}
			else {
				TransactionManager.getInstance().removeExplain(sessionId);
				return result + "\t ("+ ((end-start)/1000.0) +" sec)";
			}
			
			
		} catch (SemanticException | ParseException | TokenMgrError e) {
			return ("\n" + e.getMessage());
		} 

	}
	public void writeToClient(String msg) {

		String msgs[] = msg.split("\n");
		out.println(msgs.length);
		out.println(msg);
		
		out.flush();
	}
	@Override
	public void run() {
		try {
			while (!client.isClosed() || this.flag) {
				
				String request = null;
				if ((request = in.readLine()) != null) {
					if (request.equalsIgnoreCase("quit;")) { //out
						this.flag = false;
						break;
					} 
					else if(request.equalsIgnoreCase("commit;")){
						//AgerManager am = AgerManager.getInstance();
						//1. scn 할당 
						int scn = CommitManager.getInstance().provideSCN(sessionId);
						CommitManager.getInstance().logicalAger(sessionId, scn);
						writeToClient("커밋이 완료되었습니다.");
						
						//2. commit 완료 
					}
					else if(request.equalsIgnoreCase("rollback;")) {
						
					}
					else {
						writeToClient(doQuery(request));
					}	
				}
			}
		} catch (IOException e) {
			System.err.println("Err: Socket broken! from client");
		} finally {
			out.close();
			try {
				in.close();
			} catch (IOException e) {
				e.getStackTrace();
			}
		}
	}
}
