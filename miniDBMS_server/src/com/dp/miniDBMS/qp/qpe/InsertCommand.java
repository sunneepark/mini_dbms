package com.dp.miniDBMS.qp.qpe;

import java.util.List;
import com.dp.miniDBMS.qp.qpp.SemanticException;
import com.dp.miniDBMS.qp.qpp.Token;
import com.dp.miniDBMS.sm.smm.LogManager;
import com.dp.miniDBMS.sm.smm.MemoryDicTBSManager;
import com.dp.miniDBMS.sm.smm.MemoryTBS;
import com.dp.miniDBMS.sm.smm.MemoryTBSManager;
import com.dp.miniDBMS.sm.smo.DicColumn;
import com.dp.miniDBMS.sm.smo.DicTable;
import com.dp.miniDBMS.sm.smo.RID;
import com.dp.miniDBMS.sm.smo.type.TypeBase;
import com.dp.miniDBMS.sm.smo.type.TypeEnum;

/**
 * 
 *
 * @날짜 : Aug 6, 2020
 * @작성자 : 박선희
 * @클래스설명 : insert command 처리하는 클래스
 *
 */

public class InsertCommand extends Command {

	private String tableName;
	private List<TypeBase> values;

	private DicTable table = null;

	public InsertCommand(Token tok, String tableName, List<TypeBase> values) {
		super(tok);
		this.tableName = tableName;
		this.values = values;
	}

	@Override
	public String validate() throws SemanticException {
		
		// 1. database 선택 여부 
		String databaseName = this.getDb();
		
		MemoryDicTBSManager mdtbsm = MemoryDicTBSManager.getInstance();
		this.table = mdtbsm.validateTable(databaseName, tableName);
		
		//3. 테이블 칼럼 갯수와 타입, 범위 확인
		int i = 0;

		// 3-0. 칼럼 갯수 확인
		if (table.getColumnList().size() != values.size())
			throw new SemanticException("it is too many or small columns");

		// 3-1. 칼럼 타입 , 범위 확인
		for (DicColumn dc : table.getColumnList()) {
			TypeBase tb = values.get(i++);

			//System.out.println(tb.getValue());

			if (dc.getType().getType() != tb.getType()) { // 타입 확인
				throw new SemanticException("it will be type " + dc.getType().getType() + "in " + (i + 1) + "'th column");
			}
			if (tb.getType() == TypeEnum.CHAR) { // 범위 확인
				if (tb.getValue().toString().length() > dc.getType().getMaxSize())
					throw new SemanticException(dc.getType().getType() + " should be less than" + dc.getType().getMaxSize());
			}
		}
		
		return execute();
	}

	@Override
	public String optimize() {
		return null;
	}

	@Override
	public String execute() {
		MemoryTBSManager mtbsm = MemoryTBSManager.getInstance();
		MemoryTBS mtbs = mtbsm.getMTM(this.table);
		
		RID temp = mtbs.addTuple(values);
		
		//archive log
		LogManager lm = LogManager.getInstance();
		lm.addDirtyPage(temp);
		
		return "1 rows added.";
	}

	@Override
	public void archiveLog() {}

}
