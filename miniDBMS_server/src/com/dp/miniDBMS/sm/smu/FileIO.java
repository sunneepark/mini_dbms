package com.dp.miniDBMS.sm.smu;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
/**
  *
  * @날짜 : Aug 3, 2020 
  * @작성자 : 박선희 
  * @클래스설명 : file i/o 에 필요한 함수
  *
 */
public class FileIO {
	private static final long serialVersionUID = 8436132631907741022L;
	
	/******************** read from file *********************************/
	
	public static int readIntFromFile(ObjectInputStream ois) { //file 읽기 
	     try {
			return (ois.readInt());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 0; 
	}
	public static String readStringFromFile(ObjectInputStream ois) { //file 읽기 
	     try {
			return ois.readObject().toString();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static FileInputStream getInputStream(String fileName) { // 파일 스트림 열기 
		FileInputStream fis = null;
		try {
			File f = new File(fileName);
			if(!f.exists()) {
				try {
					f.createNewFile();
					return null;
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			fis = new FileInputStream(f);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return fis;
	}

	public static boolean closeInputStream(ObjectInputStream ois) { // 파일 닫기
		try {
			ois.close();
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	/******************** write to file *********************************/
	
	public static void writeIntToFile(int val, ObjectOutputStream oos) { //file 쓰기 
		try {
			oos.writeInt(val);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void writeStringToFile(String str, ObjectOutputStream oos) { //file 쓰기 
		try {
			oos.writeObject(str);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static FileOutputStream getOutputStream(String fileName) { // 파일 스트림 열기 
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(new File(fileName));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return fos;
	}

	public static boolean closeOutputStream(ObjectOutputStream oos) { // 파일 닫기
		try {
			oos.close();
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}
	public static FileOutputStream getOutputStream(String filePath, String fileName) {
		FileOutputStream fos = null;
		
		try {
			File f = new File(filePath);
			if(!f.exists())
				f.mkdirs();
			
			fos = new FileOutputStream(new File(filePath+fileName));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return fos;
	}

}
