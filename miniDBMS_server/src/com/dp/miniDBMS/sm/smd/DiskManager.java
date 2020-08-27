package com.dp.miniDBMS.sm.smd;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.dp.miniDBMS.sm.smo.DicTable;
import com.dp.miniDBMS.sm.smo.Page;
import com.dp.miniDBMS.sm.smu.GlobalConst;

/**
 *
 * @날짜 : Aug 20, 2020
 * @작성자 : 박선희
 * @클래스설명 : disk 로 부터 바이트 파일 쓰고 읽기
 *
 */
public class DiskManager implements GlobalConst {

	private RandomAccessFile fp;
	private int num_pages;
	private String name;

	/** default constructor. */
	public DiskManager() {
	}

	public void openDB(DicTable dt) throws FileNotFoundException {
		String filePath = GlobalConst.DATA_FILEPATH + dt.getDatabaseName();
		String fileName = "/" + dt.getTableName() +".dbf";
		
		File f = new File(filePath+fileName);
		if (!f.getParentFile().exists())
		    f.getParentFile().mkdirs();
		if (!f.exists()) {
			try {
				f.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		// 파일 액세스는 Random access file, 읽기와 쓰기 한번에 open
		fp = new RandomAccessFile(filePath+fileName, "rw");
	}

	/**
	 * db 생성
	 * 
	 * @param fname 파일이름 
	 * @param num_pgs 페이지 개수 
	 */
	public void createDB(String fname, int num_pgs) {

		name = new String(fname);
		num_pages = (num_pgs > 2) ? num_pgs : 2;

		File DBfile = new File(name);

		DBfile.delete(); // 원래 파일을 지워 ..?

		try {
			fp = new RandomAccessFile(fname, "rw");

			// 페이지 크기만큼 파일 만들고 0로 채우기
			fp.seek((long) (num_pages * PAGE_SIZE - 1));
			fp.writeByte(0);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void allWriteFile(byte[][] tbsData, int len) {
		int i = 0;
		while (i <= len) {
			try {
				fp.write(tbsData[i++]);
			} catch (IOException e) {
				e.getMessage();
			}
		}
	}
	public void write_page(Page apage) {

		try {
			fp.seek((long) (apage.pid * PAGE_SIZE));
			fp.write(apage.getData());
		} catch (IOException e) {
			e.getMessage();
		}

	}

	public int allReadFile(byte[][] tbsData) {
		long maxPID = 0; // 파일의 전체 길이

		try {
			maxPID = fp.length();

			if (maxPID == 0)
				return -1;
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		int pid = 0; // 현재 페이지 번호
		long curOffset = (long) (pid * PAGE_SIZE); // 현재 offset 위치

		while (maxPID >= (curOffset = (curOffset + (long) PAGE_SIZE))) { // 페이지 읽기 전까지
			try {
				//fp.seek(curOffset); // 페이지 별로 읽기
				fp.read(tbsData[pid]);

				pid++;
			} catch (IOException e) {
				e.toString();
			}
		}
		return pid;
	}

	public void read_page(Page apage) {
		byte[] buffer = apage.getData(); 
		try {
			fp.seek((long) (apage.pid * PAGE_SIZE));
			fp.read(buffer);
		} catch (IOException e) {
			e.toString();
		}

	}

	public void closeDB() throws IOException {
		fp.close();
	}
}


