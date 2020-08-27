package com.dp.miniDBMS.qp.qpe;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import com.dp.miniDBMS.qp.qpp.ParseException;
import com.dp.miniDBMS.qp.qpp.SQLParser;
import com.dp.miniDBMS.qp.qpp.SemanticException;
import com.dp.miniDBMS.qp.qpp.TokenMgrError;
import com.dp.miniDBMS.sm.smm.CommitManager;
import com.dp.miniDBMS.sm.smm.MemoryDicTBSManager;
/**
 * 
  *
  * @날짜 : Aug 5, 2020 
  * @작성자 : 박선희 
  * @클래스설명 : command 를 실행하는 주체 = client thread 에게 query 의 response 를 건네는 주체
  *
 */
public class CommandExecutor {
	private String databaseName;

	private static int trxId = 0;  //트랜잭션 id
	
	public String doClientExecute(String request, SQLParser parser) {	
		
		Command command = null;
		InputStream commandStream = new ByteArrayInputStream(request.getBytes());
		
		//시간 체크 시작
		
		// parsing -> syntax check
		try {
			
			parser.ReInit(commandStream);
			command = parser.Command();
			command.setTrxId(trxId++);
		
			return command.validate();
			
		} catch (SemanticException | ParseException | TokenMgrError e) {
			return "\n" + e.getMessage();
		}
		
		//validate -> semantic check
		
		//optimize -> make plan
		
		//execute -> sql result
	}
}
