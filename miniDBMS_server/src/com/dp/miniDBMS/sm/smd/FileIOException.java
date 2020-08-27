package com.dp.miniDBMS.sm.smd;

import java.io.IOException;

public class FileIOException extends IOException{
	public FileIOException(String message){ 
		super("[file io error]" + message);
	 }
}
