package com.dp.miniDBMS.sm.smm;

import java.io.File;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import com.dp.miniDBMS.sm.smd.DicDatabaseListManager;
import com.dp.miniDBMS.sm.smd.DicTableListManager;
import com.dp.miniDBMS.sm.smo.DicDatabase;
import com.dp.miniDBMS.sm.smo.DicTable;


public class TestManagers {

	public static void main(String[] args) {	
		dtlmTestWrite();
		dtlmTestRead();
	}
	public static void ddlmTest() {
		//DicDatabaseListManager db = new DicDatabaseListManager();
		
		//create db write & read
		List<DicDatabase> dblist = new ArrayList<DicDatabase>();
		for(int i=0;i<10;i++) {
			dblist.add(new DicDatabase(i, "sunny",
				LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE)));
		}
		//db.writeDatabaseToFile(dblist, Properties.DIC_DB_FILENAME);
		//db.readDatabaseFromFile(Properties.DIC_DB_FILENAME);
	}
	public static void dtlmTestWrite() {
		DicTableListManager dbt = DicTableListManager.getInstance();
//		dbt.writeDicTableListToFile(new DicDatabase(1, "sunny",
//				LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE)), Properties.DIC_TABLE_FILENAME);
//		
		//DicColumnListManager dct = DicColumnListManager.getInstance();
		//dct.writeDicColListToFile(new DicTable("sunny", 1, "snoopy", 
		//		LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-mm-dd hh:mm:ss"))), Properties.DIC_COLUMN_FILENAME);
	}
	public static void dtlmTestRead() {
		DicTableListManager dbt = DicTableListManager.getInstance();
		//dbt.readDicTableListFromFile(new DicDatabase(1, "sunny",
		//		LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE)), Properties.DIC_TABLE_FILENAME);
		
		//DicColumnListManager dct = DicColumnListManager.getInstance();
		//dct.readDicColListFromFile(new DicTable("sunny", 1, "snoopy", 
		//		LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-mm-dd hh:mm:ss"))), Properties.DIC_COLUMN_FILENAME);
	}
}
