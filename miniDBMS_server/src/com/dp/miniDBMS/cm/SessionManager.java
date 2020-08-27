package com.dp.miniDBMS.cm;

import java.util.ArrayList;
import java.util.List;

import com.dp.miniDBMS.sm.smo.DicDatabase;
/**
 * 
  *
  * @날짜 : Aug 17, 2020 
  * @작성자 : 박선희 
  * @클래스설명 : 생성된 세션들 관리하는 매니저 
  *
 */
public class SessionManager {
	private List<Session> sessions = new ArrayList<>();
	
	private static SessionManager instance = new SessionManager();
	private SessionManager() {}
	
	public static SessionManager getInstance() {
		return instance;
	}
	
	public void setSesion(Session s) {
		sessions.add(s);
	}
	public void setDB(int sessionId, DicDatabase db) {
		for(Session s : sessions) {
			if(s.getSessionId() == sessionId) {
				s.setCurrentDatabase(db);
				return;
			}
		}
	}
	public void removeSession(Session s) {
		sessions.remove(s);
	}
}
