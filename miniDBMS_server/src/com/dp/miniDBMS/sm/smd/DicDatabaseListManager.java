package com.dp.miniDBMS.sm.smd;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import com.dp.miniDBMS.sm.smo.DicDatabase;
import com.dp.miniDBMS.sm.smu.FileIO;
import com.dp.miniDBMS.sm.smu.GlobalConst;
/**
 * 
  *
  * @날짜 : Aug 3, 2020 
  * @작성자 : 박선희 
  * @클래스설명 : database 갯수와 database list 저장 
  *
 */
public class DicDatabaseListManager implements GlobalConst {
	private static Object mutex = new Object(); //observer pattern으로 변경
	 
	private static DicDatabaseListManager instance = new DicDatabaseListManager(); //멀티쓰레드 환경에서의 싱글턴 패턴 보장

	private DicDatabaseListManager() {

	}

	public static DicDatabaseListManager getInstance() {
		return instance;
	}
	
	public List<DicDatabase> readDicDBListFromFile(){ //db list 읽어오기 
		synchronized (mutex) {
			ObjectInputStream ois = null;
			List<DicDatabase> dbList = new ArrayList<DicDatabase>();
			try {
				FileInputStream fis = FileIO.getInputStream(GlobalConst.DIC_DB_FILENAME+".dat");
				if(fis == null)
					return null;
				
				ois = new ObjectInputStream(fis);
				
				if(ois == null)
					return null;
			
				int dbCnt =  FileIO.readIntFromFile(ois);
				
				for (int i=0; i < dbCnt ; i++) {
					DicDatabase db = new DicDatabase(
							FileIO.readIntFromFile(ois), FileIO.readStringFromFile(ois), FileIO.readStringFromFile(ois));
					dbList.add(db);
				}
					
			} catch (IOException e) {
				e.printStackTrace();
			} 
			return dbList;
		}
	}

	public void writeDicDBListToFile(List<DicDatabase> dbList) { //data write
		synchronized (mutex) {
			
			ObjectOutputStream oos = null;
			try {
				oos = new ObjectOutputStream(FileIO.getOutputStream(GlobalConst.DIC_DB_FILENAME+".dat"));
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			/**
			 * 
			 * [database Dictionary 값 저장]
			 * 
			 * 값 저장 형식
			 * : dbID1\tdbNAME1\tdbCreatedTime1
			 * 
			 */
			FileIO.writeIntToFile(dbList.size(),oos);
			for(DicDatabase db : dbList) {
				FileIO.writeIntToFile(db.getDatabaseId(),oos);
				FileIO.writeStringToFile(db.getDatabaseName(),oos);
				FileIO.writeStringToFile(db.getDatabaseCreatedTime(),oos);
			}

			FileIO.closeOutputStream(oos);
		}
	}
}
