package com.dp.miniDBMS.sm.smm;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

import com.dp.miniDBMS.sm.smo.RID;
import com.dp.miniDBMS.sm.smo.Tuple;

/**
 * 
 *
 * @날짜 : Aug 16, 2020
 * @작성자 : 박선희
 * @클래스설명 : insert, update, delete 시에 log 저장
 *
 */
public class LogManager {
	private HashSet<Integer> dirtyFidList = new HashSet<Integer>(); // 파일 id 에 따른 row id

	private LinkedList<Tuple> changeTupleList = new LinkedList<Tuple>();
	private LinkedList<Integer> changeIntList = new LinkedList<Integer>();

	private LinkedList<Tuple> deleteTupleList = new LinkedList<Tuple>();
	//private LinkedList<Integer> deleteIntList = new LinkedList<Integer>();

	private static LogManager instance = new LogManager();

	private LogManager() {
	}

	public static LogManager getInstance() {
		return instance;
	}

	public void addDirtyPage(RID rid) {
		dirtyFidList.add(rid.fid);
	}

	public void addLogTuple(Tuple t, int offset) {
		changeTupleList.add(t);
		changeIntList.add(offset);
		addDirtyPage(t.rid);
		return;
	}

	public void addDeleteTuple(Tuple t) {
		this.deleteTupleList.add(t);
		addDirtyPage(t.rid);
		return;
	}

	/**
	 * dirty page 가있는 테이블 flush
	 */
	public void doCheckpoint() {
		MemoryTBSManager mtbsm = MemoryTBSManager.getInstance();

		Iterator iter = dirtyFidList.iterator(); // Iterator 사용
		while (iter.hasNext()) {// 값이 있으면 true 없으면 false
			MemoryTBS tbsTemp = mtbsm.searchPage((Integer) iter.next());
			// tbsTemp.flushAll();
			tbsTemp.flushDirtyPages();
		}

	}

	/**
	 * commit 할 때 이전버전 update or delete
	 * 
	 * @param sessionID 세션 id
	 * @param scn       system commit number
	 */
	public boolean flushLog(int sessionID, int scn) {
		if (changeTupleList.size() != 0) {
			int i=0;
			Iterator<Integer> iterInt = changeIntList.iterator();
			for (Iterator<Tuple> iter = changeTupleList.iterator(); iter.hasNext();) {
				Tuple t = iter.next();
				int size = iterInt.next();
				if (t.getSessionID() == sessionID) {
					t.infomask = false;
					t.setSessionID(0);
					
					t.getNext().setScn(scn); // commit 번호 확인

					MemoryTBSManager.getInstance().searchPage(t.rid.fid).setSlot(t.rid.curOffset);
					i++;
					// 로그 삭제
					iter.remove();
					iterInt.remove();
				}
			}
		}
		if (deleteTupleList.size() != 0) {
			for (Iterator<Tuple> iter = deleteTupleList.iterator(); iter.hasNext();) {
				Tuple t = iter.next();
				if (t.getSessionID() == sessionID) {
					t.setSessionID(0);
					t.infomask = false;
					
					MemoryTBSManager.getInstance().searchPage(t.rid.fid).deleteIntList.add(t.rid.curOffset);
					// 로그 삭제
					iter.remove();
				}
			}
		}

		return true;
	}
}
