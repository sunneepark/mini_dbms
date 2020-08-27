package com.dp.miniDBMS.qp.qpp;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Scanner;

import com.dp.miniDBMS.qp.qpe.Command;
import com.dp.miniDBMS.sm.smm.LogManager;
import com.dp.miniDBMS.sm.smm.MemoryDicTBSManager;

public class TestParse {
	public static void main(String[] args) {

		MemoryDicTBSManager startMdtbm = MemoryDicTBSManager.getInstance();
		startMdtbm.loadDicTBS();
		
		SQLParser parser = new SQLParser(System.in);
		Command command = null;
		Scanner sc = new Scanner(System.in);
		String commandString = sc.nextLine();
		
		InputStream commandStream = new ByteArrayInputStream(
				commandString.getBytes());
		
		//parsing 과정 시작
		try {
			parser.ReInit(commandStream);
			command = parser.Command();
			
			System.out.println(command.validate());
		} catch (SemanticException | ParseException | TokenMgrError e) {
			System.out.println("\n" + e.getMessage());
			return;
		} 
		
		MemoryDicTBSManager mtbsm = MemoryDicTBSManager.getInstance();
		mtbsm.flush();
		
		LogManager lm = LogManager.getInstance();
		//lm.flushAll();
		
	}
}
