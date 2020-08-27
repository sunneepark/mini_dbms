package com.dp.miniDBMS.sm.smm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.dp.miniDBMS.sm.smo.DicTable;

/**
 * 
 *
 * @날짜 : Aug 7, 2020
 * @작성자 : 박선희
 * @클래스설명 : storage manager (in - memory : table space)
 * 
 *        table header 들을 관리 싱글턴으로 하나의 controller
 * 
 */

public class MemoryTBSManager {
	protected static int pageCount = 100000;
																																												// '데이터 header'
	private HashMap<String, MemoryTBS> hashTable = new HashMap<String, MemoryTBS>(); 
	
	// fid 별로 memoryTBS 저장
	private List<MemoryTBS> hashTBS = new ArrayList<MemoryTBS>();
	
	
	
	private static MemoryTBSManager instance = new MemoryTBSManager();
	private MemoryTBSManager() {}
	public static MemoryTBSManager getInstance() {
		return instance;
	}
	
	/**
	 * 테이블tbs 불러오기 
	 * 
	 * @param table 불러올 테이블 정보 
	 * @return 테이블 tbs
	 */
	public MemoryTBS getMTM(DicTable table) {

		String MTMKey = table.getDatabaseName() + "." + table.getTableName();

		if (hashTable.containsKey(MTMKey))
			return hashTable.get(MTMKey);

		// 아직 없다면
		MemoryTBS mtbs = new MemoryTBS(pageCount, hashTBS.size(), table);
		while(!mtbs.bulkLoad()); // 데이터 전부 불러오기.
		hashTable.put(MTMKey, mtbs);

		hashTBS.add(mtbs);

		return mtbs;
	}

	/**
	 * 테이블 아이디에 따른 tbs 반환 
	 * @param fid 파일 아이디 
	 * @return
	 */
	public MemoryTBS searchPage(int fid) {
		return hashTBS.get(fid);
	}

}
