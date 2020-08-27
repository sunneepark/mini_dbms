package com.dp.miniDBMS.qp.qpe;

import java.util.List;

import com.dp.miniDBMS.qp.qpp.Token;
import com.dp.miniDBMS.sm.smm.MemoryDicTBSManager;
import com.dp.miniDBMS.sm.smo.DicColumn;
import com.dp.miniDBMS.sm.smo.DicDatabase;
import com.dp.miniDBMS.sm.smo.DicTable;

/**
 * 
 *
 * @날짜 : Aug 6, 2020
 * @작성자 : 박선희
 * @클래스설명 : create table command 인 경우
 * 
 *        [경우의 수 두가지] {dbname.tablename} 일 경우 dbname 한번 더 확인 {tablename} 일 경우
 *        db는 default 로
 *
 */
public class CreateTableCommand extends Command {

	private int dbID = 0;
	private String databaseName = "default";

	private String tableName;
	private List<DicColumn> attributes;

	private DicTable dt = null;
	public CreateTableCommand(Token tok, String tableName, List<DicColumn> attributes) {
		super(tok);
		this.tableName = tableName;
		this.attributes = attributes;
	}
	@Override
	public String validate() {
		// 1. database 선택 여부
		int idx = tableName.indexOf(".");

		MemoryDicTBSManager mdtbsm = MemoryDicTBSManager.getInstance();

		DicDatabase choosenDB = new DicDatabase(dbID, databaseName);

		if (idx != -1) {
			this.databaseName = tableName.substring(0, idx);

			if ((choosenDB = mdtbsm.isExistDatabase(databaseName)) == null) { // db 존재 x
				return "[error] there is no " + databaseName + " database";
			}

			this.tableName = tableName.substring(idx + 1);
			dbID = choosenDB.getDatabaseId();
		}
		
		return execute();

	}
	@Override
	public String optimize() {
		return null;
	}

	@Override
	public String execute() {
		MemoryDicTBSManager mdtbsm = MemoryDicTBSManager.getInstance();
		
		// 3. table create
		if ((dt = mdtbsm.addTable(databaseName, tableName, attributes)) == null) {
			return "[error] duplicate exist tableName"; // table 이미 존재
		}
		System.out.println("새로 생성된 테이블 아이디 : " + dt.getTableID());

		for (DicColumn dc : attributes)
			System.out.println(dc.getColName());
		
		
		return "success table created";

	}

	@Override
	public void archiveLog() {
	}

}
