package com.dp.miniDBMS.qp.qpe;

import java.util.List;

import com.dp.miniDBMS.qp.qpo.PlanTree;
import com.dp.miniDBMS.qp.qpo.expression.Exp;
import com.dp.miniDBMS.qp.qpp.SemanticException;
import com.dp.miniDBMS.qp.qpp.Token;
import com.dp.miniDBMS.sm.smm.MemoryDicTBSManager;
import com.dp.miniDBMS.sm.smo.DicColumn;
import com.dp.miniDBMS.sm.smo.DicTable;

public class DeleteCommand extends Command {
	private int dbID = 0;

	private String tableName;
	private DicTable table = null;

	private Exp condition = null;
	private PlanTree pt = null;

	public DeleteCommand(Token tok, String tableName, Exp condition) {
		super(tok);
		this.tableName = tableName;
		this.condition = condition;
	}

	@Override
	public String validate() throws SemanticException {
		// 1. database 선택 여부
		String databaseName = this.getDb(); 
		String tableName = this.tableName;

		// 2. 테이블 정보 확인
		MemoryDicTBSManager mdtbsm = MemoryDicTBSManager.getInstance();
		this.table = mdtbsm.validateTable(databaseName, tableName);

		// 3. 테이블 칼럼 명과 타입 범위 확인
		List<DicColumn> tableColDic = table.getColumnList();

		
		pt = new PlanTree(tableColDic, 0, table, this.getscnId());
		return optimize();

	}

	@Override
	public String optimize() {
		return execute();
	}

	@Override
	public String execute() {
		String result = null;
		try {
			if(condition != null)
				condition.visitor(pt);
			
			result = pt.executeDelete(this.getSessionId()); //delete 로 실행 
			
		} catch (SemanticException e) {
			e.printStackTrace();
		}
		this.archiveLog();
		return result;
	}

	@Override
	public void archiveLog() {
		

	}
}
