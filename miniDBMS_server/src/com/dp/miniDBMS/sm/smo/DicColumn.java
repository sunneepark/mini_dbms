package com.dp.miniDBMS.sm.smo;

import com.dp.miniDBMS.sm.smo.type.TypeBase;

public class DicColumn {
	private DicTable tableDic;
	private String colName;
	private TypeBase type;

	public DicColumn(String colName, TypeBase type) {
		super();
		this.tableDic = null;
		this.colName = colName;
		this.type = type;
	}
	
	public DicColumn(DicTable tableDic, String colName, TypeBase type) {
		super();
		this.tableDic = tableDic;
		this.colName = colName;
		this.type = type;
	}

	public String getTableName() {
		return tableDic.getTableName();
	}

	public String getColName() {
		return colName;
	}

	public void setColName(String colName) {
		this.colName = colName;
	}

	public TypeBase getType() {
		return type;
	}

	public void setType(TypeBase type) {
		this.type = type;
	}
	
	
	
}
