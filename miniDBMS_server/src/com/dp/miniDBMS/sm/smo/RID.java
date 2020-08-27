package com.dp.miniDBMS.sm.smo;

import com.dp.miniDBMS.sm.smo.type.TypeBase;

/**
 * 
  *
  * @날짜 : Aug 19, 2020 
  * @작성자 : 박선희 
  * @클래스설명 : row id 식별자 
  *
 */
public class RID implements Cloneable{
	public int fid; //파일 번호 
	public int pageID; //페이지 아이디 
	public int offset; //페이지 내의 offset
	public int tuplePos; //row 'th
	public int curOffset;
	
	public RID(int fid, int pageID, int offset, int tuplePos) {
		this.fid = fid;
		this.pageID = pageID;
		this.offset = offset;
		this.tuplePos = tuplePos;
		this.curOffset = tuplePos;
	}
	
	public int getPageID() {
		return pageID;
	}
	public void setPageID(int pageID) {
		this.pageID = pageID;
	}
	public int getOffset() {
		return offset;
	}
	public void setOffset(int offset) {
		this.offset = offset;
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException{
		return (RID) super.clone();
	}
}
