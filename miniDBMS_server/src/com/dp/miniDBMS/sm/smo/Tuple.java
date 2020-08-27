package com.dp.miniDBMS.sm.smo;

import java.util.ArrayList;
import java.util.List;

import com.dp.miniDBMS.sm.smo.type.TypeBase;
import com.dp.miniDBMS.sm.smo.type.TypeEnum;

/**
  *
  * @날짜 : Aug 6, 2020 
  * @작성자 : 박선희 
  * @클래스설명 : data tuple 한 개를 관리하는 클래스 
  *
 */
public class Tuple {
	public RID rid = null;
	private int sessionID = 0; //세션 id
	private int trxID = 0; //트랜잭션 id
	private int scn = 0; //system commit number
	
	public boolean infomask = true; // 읽을 수 있는지 (virtually)
	
	private List<TypeBase> columns; //실제 데이터 tuple
	
	private Tuple next = null; //next ptr 

	public Tuple() {
		this.columns = new ArrayList<TypeBase>();
	}

	public Tuple(List<TypeBase> tuple, RID rid) {
		this.columns = tuple;
		
		this.rid = rid;
	}
	public Tuple(List<TypeBase> tuple, byte[] data, RID rid) {
		this.columns = tuple;
		this.rid = rid;
	}
	
	/*** object clonable ***/
	public Tuple(List<TypeBase> deepcopyValues, int trxId) {
		this.columns = deepcopyValues;
		this.trxID = trxId;
	}
	/**
	 * 
	 * 해당 컬럼위치의 갑을 byte 단위에서 바꿈.
	 * @param colPos 컬럼위치 
	 * @param value 바꿀 값 
	 */
	public void setValueToByte(int colPos, Object value, byte[] data ) {
		int pos = 0;
		for(int i=0;i<colPos;i++)
			pos += columns.get(i).getMaxSize();
		
		TypeEnum enumTemp = columns.get(colPos).type; //해당 타입 컬럼 
		
		if (enumTemp == TypeEnum.INT) {
			TypeEnum.INT.makeTypeWithValue(Convert.getIntValue(pos, data));

		} else if (enumTemp == TypeEnum.CHAR) {
			TypeEnum.CHAR
					.makeTypeWithValue(Convert.getStringValue(pos, data, columns.get(colPos).getMaxSize()));
		}
	}
	
	/***** getter , setter *****/
	
	public List<TypeBase> getValues() {
		return columns;
	}

	public void setValues(List<TypeBase> values) {
		this.columns = values;
	}
	public void setValue(TypeBase value, int idx) {
		this.columns.set(idx, value);
	}
	public Tuple getNext() {
		return this.next;
	}
	public void setNext(Tuple next) {
		this.next = next;
	}
	public int getTrxID() {
		return trxID;
	}

	public void setTrxID(int trxID) {
		this.trxID = trxID;
	}

	public int getScn() {
		return scn;
	}

	public int getSessionID() {
		return sessionID;
	}

	public void setSessionID(int sessionID) {
		this.sessionID = sessionID;
	}

	public void setScn(int scn) {
		this.scn = scn;
	}
	
	/**
	 * 속성 data deep copy 
	 * @return deep copy 된 속성값들 
	 */
	public List<TypeBase> deepcopyValues(){
		List<TypeBase> result = new ArrayList<TypeBase>();
		
		for (TypeBase tb : columns) {
			try {
				result.add((TypeBase)tb.clone());
			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
			}
		}
		return result;
	}
}
