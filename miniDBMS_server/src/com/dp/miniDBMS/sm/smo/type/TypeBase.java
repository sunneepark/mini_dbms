package com.dp.miniDBMS.sm.smo.type;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * 
  *
  * @날짜 : Aug 5, 2020 
  * @작성자 : 박선희 
  * @클래스설명 : 모든 컬럼 타입의 기초가 되는 클래스
  *
 */
public abstract class TypeBase implements Serializable, Cloneable{

	private static final long serialVersionUID = 8436132631907741022L;
	
	protected Object value; //타입 값 
	public TypeEnum type; //타입 종류 
	protected int maxSize; //타입 최대 사이즈 
	
	public void setMaxSize(int len) {
		this.maxSize = len;
		return;
	}
	public int getMaxSize() {
		return maxSize;
	}
	
	public TypeEnum getType() {
		return type;
	}
	public void setType(TypeEnum type) {
		this.type = type;
	}
	@Override
	public Object clone() throws CloneNotSupportedException{
		return (TypeBase) super.clone();
	}
	public abstract Object getValue(); //타입에 맞는 값 반환 
	public abstract boolean isRightSize(Object value); //값이 정해진 크기와 맞는지

	public abstract void readType(ObjectInputStream is); //파일로 부터 타입에 맞게 읽기
	public abstract void writeType(ObjectOutputStream os); //파일에 타입에 맞게 쓰기 
	
}
