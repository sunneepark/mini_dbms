package com.dp.miniDBMS.sm.smo;

import java.util.ArrayList;
import java.util.List;

import com.dp.miniDBMS.sm.smo.type.TypeBase;
/**
 * 
  *
  * @날짜 : Aug 6, 2020 
  * @작성자 : 박선희 
  * @클래스설명 : 테이블에 있는 컬럼 list 정의 
  *
 */
public class DicTable {
	private String databaseName;
	
	private int tableID;
	private String tableName;
	private String tableCreatedTime;
	
	private List<DicColumn> columnList = new ArrayList<DicColumn>();
	private int byteLen =0;
	public int indexIdx = 0;
	
	public DicTable(String databaseName, int tableID, String tableName, String tableCreatedTime) {
		super();
		this.databaseName = databaseName;
		this.tableID = tableID;
		this.tableName = tableName;
		this.tableCreatedTime = tableCreatedTime;
	}
	
	public String getDatabaseName() {
		return databaseName;
	}
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}
	public int getTableID() {
		return tableID;
	}
	public void setTableID(int tableID) {
		this.tableID = tableID;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getTableCreatedTime() {
		return tableCreatedTime;
	}
	public void setTableCreatedTime(String TableCreatedTime) {
		this.tableCreatedTime = TableCreatedTime;
	}
	public List<DicColumn> getColumnList() {
		return this.columnList;
	}
	public int getLen() {
		return this.byteLen;
	}
	public void setLen(int recordByteLen) {
		this.byteLen = recordByteLen;
	}
	public void setColumnList(List<DicColumn> attributes) {
		this.columnList = attributes;
		for(DicColumn dc : columnList) {
			byteLen += dc.getType().getMaxSize();
		}
			
	}
	
}
