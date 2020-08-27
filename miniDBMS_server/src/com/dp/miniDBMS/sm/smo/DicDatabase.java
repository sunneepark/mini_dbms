package com.dp.miniDBMS.sm.smo;

import java.io.Serializable;

/**
 * 
  *
  * @날짜 : Aug 3, 2020 
  * @작성자 : 박선희 
  * @클래스설명 : database 정보 object
  *
 */
public class DicDatabase{	

	private int databaseId;
	private String databaseName;
	private String databaseCreatedTime;
	
	public DicDatabase(int databaseId, String databaseName) {
		super();
		this.databaseId = databaseId;
		this.databaseName = databaseName;
	}
	
	public DicDatabase(int databaseId, String databaseName, String databaseCreatedTime) {
		super();
		this.databaseId = databaseId;
		this.databaseName = databaseName;
		this.databaseCreatedTime = databaseCreatedTime;
	}

	public int getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(int databaseId) {
		this.databaseId = databaseId;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public String getDatabaseCreatedTime() {
		return databaseCreatedTime;
	}

	public void setDatabaseCreatedTime(String databaseCreatedTime) {
		this.databaseCreatedTime = databaseCreatedTime;
	}
	
}
