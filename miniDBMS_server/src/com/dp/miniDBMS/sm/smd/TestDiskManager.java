package com.dp.miniDBMS.sm.smd;

import java.io.FileNotFoundException;

import com.dp.miniDBMS.sm.smo.Convert;
import com.dp.miniDBMS.sm.smo.Page;

public class TestDiskManager {
	public static void main(String[] args) {
		DiskManager disk = new DiskManager();
		int intsize = 4;
		int length = 10;
		String dbName = "data.dat";
		try {
			disk.openDB(null);
			
			Page page = new Page();
			page.setIntValue(130, 0);
			//page.setStringValue("das", 0+intsize);
			disk.write_page(page);
			
			Page twopage = new Page();
			disk.read_page(twopage);
			System.out.println(twopage.getIntValue(0));
			System.out.println(twopage.getStringValue(0+intsize,10));
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} 

	}
		 
}
