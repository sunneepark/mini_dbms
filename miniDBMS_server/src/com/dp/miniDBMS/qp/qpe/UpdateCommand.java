package com.dp.miniDBMS.qp.qpe;

import java.util.List;

import com.dp.miniDBMS.qp.qpo.PlanTree;
import com.dp.miniDBMS.qp.qpo.expression.Exp;
import com.dp.miniDBMS.qp.qpp.SemanticException;
import com.dp.miniDBMS.qp.qpp.Token;
import com.dp.miniDBMS.sm.smm.LogManager;
import com.dp.miniDBMS.sm.smm.MemoryDicTBSManager;
import com.dp.miniDBMS.sm.smo.DicColumn;
import com.dp.miniDBMS.sm.smo.DicTable;
import com.dp.miniDBMS.sm.smo.type.TypeBase;

/**
  *
  * @날짜 : Aug 19, 2020 
  * @작성자 : 박선희 
  * @클래스설명 : update command
  *
 */
public class UpdateCommand extends Command {

	private String tableName;
	private String attrName;
	
	private TypeBase value;
	private DicTable table = null;
	
	private Exp condition = null;
	private PlanTree pt = null;

	public UpdateCommand(Token tok, String tableName, String attrName, TypeBase value, Exp condition) {
		super(tok);
		this.tableName = tableName;
		this.attrName = attrName;
		this.value = value;
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

		// 3.0 update 대상 칼럼 syntax 확인
		boolean isExist = false;
		int i=0;
		for (; i < tableColDic.size(); i++) {
			String str = tableColDic.get(i).getColName();
			if (str.equals(attrName)) {
				isExist = true;
				break;
			}
		}
		
		if (!isExist)
			throw new SemanticException("threr is no column " + attrName + " in " + tableName);

		pt = new PlanTree(tableColDic, i, table,  this.getscnId());
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
			
			result = pt.executeUpdate(attrName, value, this.getTrxId(), this.getSessionId() ); //update 로 실행 
			
		} catch (SemanticException e) {
			e.printStackTrace();
		}
		this.archiveLog();
		return result;
	}

	@Override
	public void archiveLog() {
//		LogManager lm = LogManager.getInstance();
//		lm.addLog(table);
	}

}
