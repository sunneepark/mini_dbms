package com.dp.miniDBMS.sm.smd;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import com.dp.miniDBMS.sm.smo.DicColumn;
import com.dp.miniDBMS.sm.smo.DicTable;
import com.dp.miniDBMS.sm.smo.type.TypeBase;
import com.dp.miniDBMS.sm.smu.FileIO;
import com.dp.miniDBMS.sm.smu.GlobalConst;

/**
 * 
  *
  * @날짜 : Aug 5, 2020 
  * @작성자 : 박선희 
  * @클래스설명 : 데이터베이스 당 테이블, column list 조회 
  *
 */
public class DicTableListManager implements GlobalConst{
	private static Object mutex = new Object(); 

	private static DicTableListManager instance = new DicTableListManager(); //멀티쓰레드 환경에서의 싱글턴 패턴 보장

	private DicTableListManager() {

	}

	public static DicTableListManager getInstance() {
		return instance;
	}
	
	public static List<DicTable> readDicTableListFromFile(){ //table 읽어오기
		synchronized (mutex) {
			String fileName = GlobalConst.DIC_TABLE_FILENAME+".dat";
			List<DicTable> dbTableList = new ArrayList<DicTable>();
			
			ObjectInputStream ois = null;
			try {
				FileInputStream fis = FileIO.getInputStream(fileName);
				if(fis == null)
					return null;
				
				ois = new ObjectInputStream(fis);
				
				if(ois == null)
					return null;
				
				int tableCnt =  FileIO.readIntFromFile(ois);
				
				for (int i=0; i < tableCnt ; i++) {
					/*** table 정보 읽기 ***/
					DicTable dct = new DicTable(FileIO.readStringFromFile(ois),
							FileIO.readIntFromFile(ois), FileIO.readStringFromFile(ois), FileIO.readStringFromFile(ois));
					dbTableList.add(dct);
					
					
					/*** column 읽기 ***/
					int colCnt =  FileIO.readIntFromFile(ois);
					List<DicColumn> tableColList = new ArrayList<DicColumn>(); //db column list 
					
					for (int j=0; j < colCnt ; j++) {
						String colName =  FileIO.readStringFromFile(ois);
						TypeBase colType = null;
						try {
							colType = (TypeBase) ois.readObject();
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						}
						DicColumn dc = new DicColumn(colName , colType);
						tableColList.add(dc);
					}
					dct.setColumnList(tableColList);
				}
						
			} catch (IOException e) {
				e.printStackTrace();
			} 
			return dbTableList;
		}
	}

	//데이터베이스 별 테이블 전부 
	public static void writeDicTableListToFile(List<DicTable> dbTableList) { //data write
		synchronized (mutex) {
			String fileName = GlobalConst.DIC_TABLE_FILENAME+".dat";
			
			ObjectOutputStream oos = null;
			try {
				oos = new ObjectOutputStream(FileIO.getOutputStream(fileName));
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			/**
			 * 
			 * [table Dictionary 값 저장]
			 * 
			 * 값 저장 형식
			 * : tbID1\tbNAME1\tbCreatedTime1
			 * 
			 */
			FileIO.writeIntToFile(dbTableList.size(),oos); //테이블 갯수 
			
			for(DicTable dbt : dbTableList) {
				FileIO.writeStringToFile(dbt.getDatabaseName(),oos);
				FileIO.writeIntToFile(dbt.getTableID(),oos);
				FileIO.writeStringToFile(dbt.getTableName(),oos);
				FileIO.writeStringToFile(dbt.getTableCreatedTime(),oos);
				
				List<DicColumn> columnList = dbt.getColumnList();
				/**
				 * 
				 * [column Dictionary 값 저장]
				 * 
				 * 값 저장 형식
				 * : columnNAME\tcolumnTYPE1\t
				 * 
				 */
				FileIO.writeIntToFile(columnList.size(),oos); //컬럼 갯수 저장
				for(DicColumn col : columnList) {
					FileIO.writeStringToFile(col.getColName(),oos);
					col.getType().writeType(oos);
				}
			}
			
			

			FileIO.closeOutputStream(oos);
		}
	}
}
