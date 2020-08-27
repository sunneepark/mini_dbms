package com.dp.miniDBMS.sm.smo.type;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.dp.miniDBMS.sm.smu.FileIO;

public class TypeChar extends TypeBase{
	
	public TypeChar(TypeEnum type) {
		this.type = type;
		this.maxSize = 0;
	}
	
	public TypeChar(TypeEnum c, Object value) {
		this.type = c;
		this.value = value;
	}

	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}
	@Override
	public void readType(ObjectInputStream is) {
		
	}

	@Override
	public void writeType(ObjectOutputStream os) { //type 자체 write
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


	@Override
	public boolean isRightSize(Object value) {
		return true;
	}
	
}
