package com.dp.miniDBMS.sm.smm;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import com.dp.miniDBMS.qp.qpp.SemanticException;
import com.dp.miniDBMS.sm.smd.DicDatabaseListManager;
import com.dp.miniDBMS.sm.smd.DicTableListManager;
import com.dp.miniDBMS.sm.smo.DicColumn;
import com.dp.miniDBMS.sm.smo.DicDatabase;
import com.dp.miniDBMS.sm.smo.DicTable;

/**
 *
 * @날짜 : Aug 21, 2020
 * @작성자 : 박선희
 * @클래스설명 : dictionary 파일 한번에 읽기
 * 
 *        -information_schema 중 db, table 스키마 객체 정보(meta data)가 들어가 있음.
 *
 */
public class MemoryDicTBSManager {
	private static List<DicDatabase> dbList = new ArrayList<DicDatabase>(); // db list 확인.

	private HashMap<String, List<DicTable>> dbTableList = new HashMap<String, List<DicTable>>(); // key : dbname, value
																									// : table 리스트
	private HashMap<String, DicTable> tableData = new HashMap<String, DicTable>(); // KEY : databaseName.tableName ,
																					// VALUE : table 각각의 dictionary

	private static MemoryDicTBSManager instance = new MemoryDicTBSManager();

	private MemoryDicTBSManager() {}

	public static MemoryDicTBSManager getInstance() {
		return instance;
	}

	public DicDatabase isExistDatabase(String dbName) { // 이미 있는 db 이름인지
		int i;
		for (DicDatabase db : dbList) {
			if (db.getDatabaseName().equals(dbName))
				return db;
		}
		return null;
	}

	public boolean addDatabase(String dbName) {

		if (isExistDatabase(dbName) != null)
			return false;

		dbList.add(new DicDatabase(dbList.size(), dbName,
				LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-mm-dd hh:mm:ss"))));

		return true;

	}

	/**
	 * 테이블 조작에 관련된 함수들 0. 이미 존재하는 테이블 인지 1. 테이블 추가
	 * 
	 * @param tableName 존재하는 테이블 이름 확인.
	 * @return
	 */
	public DicTable isExistTable(String dbName, String tableName) {

		if (!dbTableList.containsKey(dbName)) { // db 자체가 없을 때
			return null;
		}
		List<DicTable> dbTableList = this.dbTableList.get(dbName);

		if (dbTableList == null)
			return null;

		int i;
		for (DicTable dbt : dbTableList) {
			if (dbt.getTableName().equals(tableName))
				return dbt;
		}
		return null;
	}

	public DicTable addTable(String dbName, String tableName, List<DicColumn> attributes) {
		if (isExistTable(dbName, tableName) != null) { // 테이블이 존재하면 추가하지 않음.
			return null;
		}

		List<DicTable> temp = this.dbTableList.get(dbName);
		if (temp == null)
			temp = new LinkedList<DicTable>();

		DicTable dt = new DicTable(dbName, temp.size(), tableName,
				LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-mm-dd hh:mm:ss")));

		dt.setColumnList(attributes); // 컬럼 추가

		temp.add(dt); // 테이블 추가
		this.dbTableList.put(dbName, temp);
		this.tableData.put(dbName + "." + tableName, dt);

		return dt;
	}

	public DicTable validateTable(String databaseName, String tableName) throws SemanticException {
		DicTable result = null;
		int idx = tableName.indexOf(".");

		if (databaseName == null) {
			databaseName = "default";
		}

		if (idx != -1) { // . 이 포함되어 있을 때
			databaseName = tableName.substring(0, idx);

			if (isExistDatabase(databaseName) == null) { // db 존재 x
				throw new SemanticException("there is no " + databaseName + " database");
			}

			tableName = tableName.substring(idx + 1);
		}

		// 2. 테이블 정보 파악
		if ((result = isExistTable(databaseName, tableName)) == null)
			throw new SemanticException("there is no " + tableName + " table in " + databaseName);

		return result;

	}

	/**
	 * 
	 * table dictionary 불러오기 1. 데이터 베이스 list 불러오기 2. 데이터 베이스 당 테이블 list 불러오기
	 * 
	 */
	@SuppressWarnings("static-access")
	public void loadDicTBS() {

		// 데이터베이스 list 불러오기
		DicDatabaseListManager db = DicDatabaseListManager.getInstance();

		this.dbList = db.readDicDBListFromFile();

		if (this.dbList == null) {
			this.dbList = new ArrayList<DicDatabase>();
			this.dbList.add(new DicDatabase(0, "DEFAULT", LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE)));
		}

		// 데이터베이스 테이블 list 정보 불러오기
		DicTableListManager dtl = DicTableListManager.getInstance();
		List<DicTable> currentDBTableList = dtl.readDicTableListFromFile();

		if (currentDBTableList == null)
			return;
		
		//데이터베이스 별 테이블 list 넣기
		for(DicTable dt : currentDBTableList) {
			List<DicTable> temp = dbTableList.get(dt.getDatabaseName());
			if(temp == null)
				temp = new ArrayList<DicTable>();
			temp.add(dt);
			dbTableList.put(dt.getDatabaseName(), temp); 
			
			tableData.put(dt.getDatabaseName()+"."+dt.getTableName(), dt);
		}
	}
	
	/**
	 * table dictionary 저장 
	 */
	public void flush() {
		
		// 1.데이터 베이스 list flush
		DicDatabaseListManager db = DicDatabaseListManager.getInstance();
		db.writeDicDBListToFile(dbList);
		
		// 2.데이터 베이스에 따른 table list와 column list flush
		DicTableListManager dtl = DicTableListManager.getInstance();
		
		Iterator dbtableIT = dbTableList.entrySet().iterator();
		List<DicTable> allTables = new ArrayList<DicTable>();
		
		while(dbtableIT.hasNext()) {
			Entry tempTable = (Entry)dbtableIT.next();
			allTables.addAll((List<DicTable>)tempTable.getValue()); //데이터베이스에 따른 테이블 리스트 
		}
		
		dtl.writeDicTableListToFile(allTables);
	}
}
