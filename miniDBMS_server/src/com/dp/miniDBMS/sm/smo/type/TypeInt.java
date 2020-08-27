package com.dp.miniDBMS.sm.smo.type;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.dp.miniDBMS.sm.smu.FileIO;

public class TypeInt extends TypeBase {
	private static final long serialVersionUID = 1L;


	public TypeInt(TypeEnum type) {
		this.type = type;
		this.maxSize = 4;
	}
	
	
	public TypeInt(TypeEnum i, Object value) {
		this.type = i;
		this.value = value;
	}

	@Override
	public void readType(ObjectInputStream is) {
		
	}

	@Override
	public void writeType(ObjectOutputStream os) {
		try {
			os.writeObject(this);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	@Override
	public Object getValue() {
		return value;
	}
	public void setValue(Object value) {
		this.value = Integer.valueOf((String)value);
	}

	@Override
	public boolean isRightSize(Object value) {
		return true;
	}

}
