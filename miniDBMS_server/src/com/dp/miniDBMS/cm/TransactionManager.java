package com.dp.miniDBMS.cm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.dp.miniDBMS.sm.smm.CommitManager;

/**
 * 
  *
  * @날짜 : Aug 17, 2020 
  * @작성자 : 박선희 
  * @클래스설명 : 세션마다 트랜잭션 관리
  *
 */
public class TransactionManager {
	private List<Integer> trxList = new ArrayList<>(); // 트랜잭션마다 세션 아이디 기록 
	private List<Integer> viewSCNList = new ArrayList<>(); // 트랜잭션마다 view scn 기
	private static int trxId = 0; //트랜잭션 아이디
	
	private HashMap<Integer, String> explainList = new HashMap<Integer, String>(); //트랜잭션(세션) 별 실행계획 통계자료
	
	private static TransactionManager instance = new TransactionManager();
	private TransactionManager() {}
	
	public static TransactionManager getInstance() {
		return instance;
	}
	
	public int makeTrx(int sessionId) {
		trxList.add(sessionId);
		viewSCNList.add(CommitManager.viewSCN);
		return trxId++;
	}
	public String getExplain(int sessionId) {
		String temp = "";
		
		String msgs[] = explainList.getOrDefault(sessionId,"").split("\n");

		for(int i=0;i < msgs.length;i++) {
			for(int j=0;j<i;j++)
				temp += "\t";
			
			temp+=msgs[i];
			
			if(i+1 != msgs.length)
				temp += "\n";
		}
		explainList.put(sessionId, "");
		return temp;
	}
	public void removeExplain(int sessionId) {
		explainList.put(sessionId,"");
	}
	public void setExplain(int sessionId, String explain) {
		String temp = explainList.getOrDefault(sessionId, "");
		explainList.put(sessionId, explain.concat(temp));
	}
	/**
	 * 트랜잭션을 하고 있는 session id
	 * @param trxId 알고자 하는 트랜잭션 아이디 
	 * @return
	 */
	public int getSessionId(int trxId) {
		return trxList.get(trxId);
	}
	public int getViewSCN(int trxId) {
		return viewSCNList.get(trxId);
	}
}
