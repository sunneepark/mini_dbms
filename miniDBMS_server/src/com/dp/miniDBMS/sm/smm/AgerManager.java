package com.dp.miniDBMS.sm.smm;
/**
 * 
  *
  * @날짜 : Aug 17, 2020 
  * @작성자 : 박선희 
  * @클래스설명 : update, delete 시 ager manager
  * 
  * 1. ager manager 
  *
 */
public class AgerManager {
	
	private static AgerManager instance = new AgerManager();
	private AgerManager() {}
	
	public static AgerManager getInstance() {
		return instance;
	}

}
