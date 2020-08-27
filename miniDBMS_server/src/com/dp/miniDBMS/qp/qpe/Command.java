package com.dp.miniDBMS.qp.qpe;


import com.dp.miniDBMS.qp.qpp.SemanticException;
import com.dp.miniDBMS.qp.qpp.Token;
import com.dp.miniDBMS.sm.smm.CommitManager;


public abstract class Command{
	private Token tok;
	
	private int trxId; //트랜잭션 아이디  
	private int sessionId; //세션 아이디 
	private int viewSCN = CommitManager.viewSCN; //view scn
	private String dbName = null;
	
	public boolean isExplain = false;

	public Command(Token tok) { 
		this.tok = tok;
	}
	public void setTrxId(int trxId) {
		this.trxId = trxId;
	}
	public int getTrxId() {
		return this.trxId;
	}
	public int getscnId() {
		return this.viewSCN;
	}
	public int getSessionId() {
		return sessionId;
	}
	public void setSessionId(int sessionId) {
		this.sessionId = sessionId;
	}
	public String getDb() {
		return dbName;
	}
	public void setDb(String db) {
		this.dbName = db;
	}
	public abstract String validate() throws SemanticException;
	public abstract String optimize();
	public abstract String execute();
	public abstract void archiveLog();
}
