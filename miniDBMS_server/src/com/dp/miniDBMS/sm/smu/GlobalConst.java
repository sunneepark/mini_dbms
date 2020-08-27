package com.dp.miniDBMS.sm.smu;

/**
  *
  * @날짜 : Aug 21, 2020 
  * @작성자 : 박선희 
  * @클래스설명 : 설정 변수 
  *
 */
public interface GlobalConst {
	//
	// Disk Manager Constants
	//

	/** Size of a page, in bytes. */
	public static final int PAGE_SIZE = 1024;

	
	//
	// FILE 정보 
	//

	public static final String ROOT_PATH = "/root/";

	public static final String DIC_FILEPATH = ROOT_PATH+"dictionary/";
	public static final String DATA_FILEPATH = ROOT_PATH+"data/";

//	public static final String DIC_FILEPATH = "dictionary/";
//	public static final String DATA_FILEPATH = "data/";

	// public static final String DIC_DB_FILENAME = ROOT_PATH+ "DB";
	public static final String DIC_DB_FILENAME = DIC_FILEPATH+"DB";
	public static final String DIC_TABLE_FILENAME = DIC_FILEPATH + "SCHEMA";
}
