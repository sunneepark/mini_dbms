package com.dp.miniDBMS.qp.qpe;

import java.util.ArrayList;
import java.util.List;

import com.dp.miniDBMS.cm.TransactionManager;
import com.dp.miniDBMS.qp.qpo.PlanTree;
import com.dp.miniDBMS.qp.qpo.expression.Exp;
import com.dp.miniDBMS.qp.qpp.SemanticException;
import com.dp.miniDBMS.qp.qpp.Token;
import com.dp.miniDBMS.sm.smm.MemoryDicTBSManager;
import com.dp.miniDBMS.sm.smo.DicColumn;
import com.dp.miniDBMS.sm.smo.DicTable;

/**
 * 
 *
 * @날짜 : Aug 11, 2020
 * @작성자 : 박선희
 * @클래스설명 : select 커맨드
 *
 */
public class SelectCommand extends Command {

	List<String> attrNames = null; // select 대상 속성 이름.
	int[] attrIdxes = null;
	int type = -1; // 0 : * , 2 : count(*)

	List<String> tables = new ArrayList<String>(); // from 대상 테이블 이름.
	Exp condition = null; // expression tree

	String orderCol = null; // order 대상 컬럼 이름.
	String ignoreCol = "0"; // inex 대상 컬럼 이름 .
	int orderIdx = -1;
	DicTable table = null;

	PlanTree pt = null;

	public SelectCommand(Token tok, List<String> attrNames, int type, List<String> tables, Exp condition,
			String orderCol, String ignoreCol, boolean isExplain) {
		super(tok);

		if (attrNames != null) {
			this.attrNames = attrNames;
			attrIdxes = new int[this.attrNames.size()];
		}

		this.type = type;
		this.tables = tables;
		this.condition = condition;
		this.orderCol = orderCol;
		this.ignoreCol = ignoreCol;
		this.isExplain = isExplain;
	}

	@Override
	public String validate() throws SemanticException {

		// 1. database 선택 여부
		String databaseName = this.getDb();
		System.out.println("chosen db" + databaseName);
		// 2. 테이블 정보 확인
		String tableName = tables.get(0);

		MemoryDicTBSManager mdtbsm = MemoryDicTBSManager.getInstance();
		this.table = mdtbsm.validateTable(databaseName, tableName);

		// 3. 테이블 칼럼 명과 타입 범위 확인
		List<DicColumn> tableColDic = table.getColumnList();

		// 3.0 select 대상 칼럼 syntax 확인
		if (type == -1) {
			int idx = 0;
			for (String attr : attrNames) {
				boolean isExist = false;
				for (int i = 0; i < tableColDic.size(); i++) {
					String str = tableColDic.get(i).getColName();

					if (str.equals(orderCol)) {
						orderIdx = i;
					}
					if (str.equals(attr)) {
						isExist = true;
						this.attrIdxes[idx++] = i;
						break;
					}
				}
				if (!isExist)
					throw new SemanticException("threr is no column " + attr + " in " + tables.get(0));
			}
			if(orderCol != null) {
				for (int i = 0; i < tableColDic.size(); i++) {
					String str = tableColDic.get(i).getColName();

					if (str.equals(orderCol)) {
						System.out.println(orderCol+" "+i);
						this.orderIdx = i;
						break;
					}
				}
				if (orderIdx == -1)
					throw new SemanticException("threr is no column " + orderCol + " in " + tables.get(0)+" so, you can't ordering");
			}
			
			
		}
		// 3.1 * 이면 모든 컬럼 출력
		else if (type == 1) {
			int size = tableColDic.size();
			this.attrNames = new ArrayList<String>(size);
			attrIdxes = new int[size];

			for (int i = 0; i < size; i++) {
				if (tableColDic.get(i).getColName().equals(orderCol)) {
					orderIdx = i;
				}
				this.attrNames.add(tableColDic.get(i).getColName());
				attrIdxes[i] = i;
			}

		}

		pt = new PlanTree(tableColDic, attrIdxes, table, this.getscnId());

		// try {
		if (condition != null) // 조건이 있을 경우 visitor 돌면서 node 생성
			condition.visitor(pt);
//		} catch (SyntaxException e) {
//			throw newzΩzzz;
//		}

		return optimize();
	}

	/**
	 * optimize 대상
	 * 
	 * 1. count(*) 가 있는데 where이 없는 경우 > 메모리 테이블 딕셔너리 참조해서 전체 갯수 가져오기. 2. order by 가
	 * 있는데 where이 없는 경우 > 메모리 테이블 바로 읽기
	 * 
	 * 1. order by 인덱스 , where 절은 인덱스가 없을 때 2. c2 and c1 일 때
	 * 
	 */
	@Override
	public String optimize() {
		String result = "";

		
		List<DicColumn> tableColDic = table.getColumnList();

		// 1. c2 and c1 일때
		if (pt.logical == 1 && !ignoreCol.equalsIgnoreCase("C1")) { // and 일때, ignore index 가 아닐 때
			for (int j = 0; j < tableColDic.size(); j++) {
				String str = tableColDic.get(j).getColName();

				if (str.equals(pt.qu.get(0).attr)) {
					pt.qu.get(0).attrIdx = j;
					break;
				}
			}

			if (pt.qu.get(0).attrIdx != 0) { // where 절 처음이 인덱스가 아닐 때.
				pt.qu.add(pt.qu.poll()); // 뒤로 미루기
			}
		}
		
		// 2. order by 인덱스 , where 절은 인덱스가 없을 때
		if (orderIdx == 0) { // order by 절이 인덱스 일 때
			
			if(pt.qu.size() == 0 || pt == null) { //order by만 있을 때 .
				
				return pt.printAllResult(pt.byrowid(pt.executeOrder(orderCol),this.getSessionId(), pt.viewSCN));
			}
			if (pt.qu.size() == 2) {
				pt.optimize = true;
				return execute();
			}
			
			for (int i = 0; i < pt.qu.size(); i++) { // where 절 인덱스 여부 확인
				for (int j = 0; j < tableColDic.size(); j++) {
					String str = tableColDic.get(j).getColName();
					if (str.equals(pt.qu.get(i).attr)) {
						pt.qu.get(i).attrIdx = j;
						break;
					}
				}
			
				if (pt.qu.get(i).attrIdx != 0 && (ignoreCol.compareToIgnoreCase("C1") != 0)) { // 인덱스가 아닐때 : where c2  order by c1
					TransactionManager.getInstance().setExplain(this.getSessionId(), "full index scan\n");
					
					return pt.printAllResult(
							pt.qu.get(i).nextFetch(table, pt.byrowid(pt.executeOrder(orderCol),this.getSessionId(), pt.viewSCN)
									, this.getSessionId(), pt.viewSCN));
				} else if (pt.qu.get(i).attrIdx == 0){ // 인덱스 일 때 : where c1 order by c1
					pt.optimize = true;
					pt.orderCol = orderCol;
				} else { // ignore c1 where c2 order by c1
					pt.orderCol = orderCol;
					pt.orderColIdx = orderIdx;
				}
			}
		} else if (orderCol != null) { // 인덱스가 아닌 order 절 : {} order by c2
			//if (pt.qu.size() == 0) {
			//	return "지원하지 않는 기능입니다.\n";
			//} else { // where 절이 있을 경우
				pt.orderCol = orderCol;
				pt.orderColIdx = orderIdx;
			//}
		}

		return execute();
	}

	@Override
	public String execute() {
		String result = null;

		if (type == 2) // count(*) 일 때
			result = pt.countAll(this.getSessionId());
		else
			result = pt.execute(attrNames, this.getSessionId());

		return result;
	}

	@Override
	public void archiveLog() {
	}

}
