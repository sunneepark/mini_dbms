package com.dp.miniDBMS.sm.smm;

import java.util.ArrayList;
import java.util.List;

/**
  *
  * @날짜 : Aug 16, 2020 
  * @작성자 : 박선희 
  * @클래스설명 : commit command 의 프로세스를 관리하는 클래스 
  *
 */
public class CommitManager {
	//insert, update, delete 시에 변경된 내용을 저장 > log manager
	 
	public static int viewSCN = 0;
	private static int SCN = 0;
	
	private List<Integer> scnMap = new ArrayList<>();
	private static CommitManager instance = new CommitManager();
	private CommitManager() {}
	
	public static CommitManager getInstance() {
		return instance;
	}
	
	public void logicalAger(int sessionID, int scn) {
		LogManager lm = LogManager.getInstance();
		while(!lm.flushLog(sessionID, scn));
		
		trigger(scn);
	}
	public int provideSCN(int sessionID) {
		scnMap.add(sessionID);
		return ++SCN;
	}
	/**
	 * commit 번호 증가 
	 * 
	 * @param scn system commit number
	 */
	public void trigger(int scn) {
		if(viewSCN < scn)
			viewSCN = scn;
	}
}
