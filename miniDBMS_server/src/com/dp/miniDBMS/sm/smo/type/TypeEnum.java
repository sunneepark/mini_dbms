package com.dp.miniDBMS.sm.smo.type;

/**
 * 
  *
  * @날짜 : Aug 6, 2020 
  * @작성자 : 박선희 
  * @클래스설명 : column type의 enum을 묶어서 상태에 따른 다른 행동을 한번에 관리
  *
 */
public enum TypeEnum {
	
	INT{
		@Override
		public TypeBase makeType() {return new TypeInt(INT);}

		@Override
		public TypeBase makeTypeWithValue(Object value) { return new TypeInt(INT, value);}
	},
	CHAR{
		@Override
		public TypeBase makeType() {return new TypeChar(CHAR);}

		@Override
		public TypeBase makeTypeWithValue(Object value) { return new TypeChar(CHAR, value);}
	};
	
	public abstract TypeBase makeType();
	public abstract TypeBase makeTypeWithValue(Object value);
	
}
