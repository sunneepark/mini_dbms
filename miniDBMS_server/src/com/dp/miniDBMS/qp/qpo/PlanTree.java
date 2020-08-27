package com.dp.miniDBMS.qp.qpo;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import com.dp.miniDBMS.cm.TransactionManager;
import com.dp.miniDBMS.qp.qpe.PlanNode;
import com.dp.miniDBMS.qp.qpe.SortBuffer;
import com.dp.miniDBMS.sm.smm.LogManager;
import com.dp.miniDBMS.sm.smm.MemoryTBS;
import com.dp.miniDBMS.sm.smm.MemoryTBSManager;
import com.dp.miniDBMS.sm.smo.DicColumn;
import com.dp.miniDBMS.sm.smo.DicTable;
import com.dp.miniDBMS.sm.smo.RID;
import com.dp.miniDBMS.sm.smo.Tuple;
import com.dp.miniDBMS.sm.smo.type.TypeBase;

/** 
  *
  * @날짜 : Aug 10, 2020 
  * @작성자 : 박선희 
  * @클래스설명 : where 절이 있을 경우 plan tree 에 따라 쿼리문 실행 
  *
 */
public class PlanTree {
	public int logical = -1; // 1: and , 0:or

	DicTable dt = null;
	public List<DicColumn> tableColDic = null;
	int[] attrIdxes = null;
	
	public int viewSCN = 0;

	public DicColumn attrTemp = null;
	public LinkedList<PlanNode> qu = new LinkedList<PlanNode>();
	public String orderCol="";
	public boolean optimize = false;

	public int orderColIdx =0 ;

	public PlanTree(List<DicColumn> tableColDic, int[] attrIdxes, DicTable dt, int viewSCN) {
		this.tableColDic = tableColDic;
		this.attrIdxes = attrIdxes;
		this.dt = dt;
		this.viewSCN = viewSCN;
	}

	public PlanTree(List<DicColumn> tableColDic, int attrIdx, DicTable dt, int viewSCN) {
		this.tableColDic = tableColDic;
		this.attrIdxes = new int[1];
		this.attrIdxes[0] = attrIdx;
		this.dt = dt;
		this.viewSCN = viewSCN;
	}

	public void setLogical(int idx) {
		this.logical = idx;
	}

	// select count(*) 전체 결과의 갯수를 세는 함수
	public String countAll(int sessionId) {
		PlanNode pn = qu.poll();

		List<Tuple> tuple = null;
		MemoryTBSManager mtbsm = MemoryTBSManager.getInstance();
		MemoryTBS mtm = mtbsm.getMTM(dt);
		
		if (pn == null) {
			tuple = mtm.getByRID(mtm.getTuples(), sessionId, viewSCN);
			TransactionManager.getInstance().setExplain(sessionId, "full table scan\n");
			
		} else {	
			tuple = pn.fetchPlanNode(dt, sessionId, viewSCN);
			while ((pn = qu.poll()) != null) {
				if (this.logical == 1) { //and 일 경우 
					String temp = pn.getAttr();
					for (int i = 0; i < tableColDic.size(); i++) { // attribute idx 찾기
						if (temp.equals(tableColDic.get(i).getColName())) {
							pn.attrIdx = i;
							break;
						}
					}
					tuple = pn.nextFetch(dt, tuple, sessionId, viewSCN);
				} else if (this.logical == 0) //or 일 경우 
					tuple = doOrExecute(pn, tuple, sessionId);
			}
		}
		
		/**** order 처리 ***/
		//order 처리 필요없음.
		/**
		 * 결과 display 로 바꾸는 부분
		 */
		StringBuffer bf = new StringBuffer();

		bf.append("\t");
		bf.append("count(*)");
		bf.append("\n");
		bf.append("\t");
		bf.append(tuple.size());
		bf.append("\n");
		
		return bf.toString();
	}

	//order by 
	public List<Tuple> executeOrder(String orderCol) {
		MemoryTBSManager mtbsm = MemoryTBSManager.getInstance();
		MemoryTBS mtm = mtbsm.getMTM(dt);
		return mtm.getAllSortedValues(orderCol); //인덱스 order 먼저 하고 
	}
	
	// select 결과를 반환하는 함수
	public String execute(List<String> attrNames, int sessionId) {
		PlanNode pn = qu.poll();

		List<Tuple> tuple = null;
		
		MemoryTBSManager mtbsm = MemoryTBSManager.getInstance();
		MemoryTBS mtm = mtbsm.getMTM(dt);
	
		if (pn == null) {
			tuple = mtm.getByRID(mtm.getTuples(), sessionId, viewSCN);
			TransactionManager.getInstance().setExplain(sessionId, "full table scan\n");
			
		} else {	
			tuple = pn.fetchPlanNode(dt, sessionId, viewSCN);
			while ((pn = qu.poll()) != null) {
				if(tuple == null)
					return printAllResult(tuple);
				
				if (this.logical == 1) { //and 일 경우 
					String temp = pn.getAttr();
					for (int i = 0; i < tableColDic.size(); i++) { // attribute idx 찾기
						if (temp.equals(tableColDic.get(i).getColName())) {
							pn.attrIdx = i;
							break;
						}
					}
					tuple = pn.nextFetch(dt, tuple, sessionId, viewSCN);
				} else if (this.logical == 0) //or 일 경우 
					tuple = doOrExecute(pn, tuple, sessionId);
			}
		}
		
		/**** order 처리 ***/
		if(orderCol.length() != 0) { 
			if(optimize) {
				return printAllResult(tuple);
			}
			else if(!optimize){
				//if(pn == null)	
				//tuple = mtm.getByRID(tuple, sessionId, viewSCN);
				
				System.out.println("order col: "+orderColIdx);
				List<Tuple> temp = new LinkedList<Tuple>();
				temp = tuple.stream().collect(Collectors.toList());
				
				new SortBuffer().quickSort(temp, sessionId, orderColIdx);
				
				//System.out.println("using temporary");
				
				return printAllResult(temp);
			}
		}
		return printAllResult(tuple);
	}
	
	/**
	 * tuple 의 결과를 string 으로 변환 
	 * 
	 * @param tuple 쿼리 결과 
	 * @return
	 */
	public String printAllResult(List<Tuple> tuple) {
		if (tuple == null || tuple.size() == 0)
			return "\n0 row selected.";
		/**
		 * 결과 display 로 바꾸는 부분
		 */
		StringBuffer bf = new StringBuffer();

		for (int idx : attrIdxes) {
			bf.append("\t");
			String put = tableColDic.get(idx).getColName();
			bf.append(put);
		}
		
		bf.append("\n");
		
		for (Tuple record : tuple) {
			bf.append("\t");
			
			for (int idx : attrIdxes)
				bf.append(record.getValues().get(idx).getValue().toString() + "\t");
			bf.append("\n");
		}

		bf.append("\n" + tuple.size());

		if (tuple.size() == 1)
			bf.append(" row selected.");
		else
			bf.append(" rows selected.");
		return bf.toString();
		
	}
	// or 일 경우에
	public List<Tuple> doOrExecute(PlanNode pn, List<Tuple> prior, int sessionId) {
		List<Tuple> curResult = pn.fetchPlanNode(dt, sessionId, viewSCN);
		curResult.removeAll(prior);
		curResult.addAll(prior);

		return curResult;
	}

	//update
	public String executeUpdate(String attrName, TypeBase value, int trxId, int sessionId) {
		PlanNode pn = qu.poll();
		
		MemoryTBSManager mtbsm = MemoryTBSManager.getInstance();
		MemoryTBS mtm = mtbsm.getMTM(dt);
		
		List<Tuple> tuple = null;
		if (pn == null) {
			tuple = mtm.getByRID(mtm.getTuples(), sessionId, viewSCN);
			
		} else {
			tuple = pn.fetchPlanNode(dt, sessionId, viewSCN);
			while ((pn = qu.poll()) != null) {
				if (this.logical == 1) {
					String temp = pn.getAttr();
					for (int i = 0; i < tableColDic.size(); i++) { // attribute idx 찾기
						if (temp.equals(tableColDic.get(i).getColName())) {
							pn.attrIdx = i;
							break;
						}
					}
					tuple = pn.nextFetch(dt, tuple, sessionId, viewSCN);
				} else if (this.logical == 0)
					tuple = doOrExecute(pn, tuple, sessionId);
			}
			tuple = mtm.getByRID(tuple, sessionId, sessionId);
		}

		if (tuple == null || tuple.size() == 0)
			return "\n0 row changed.";

		LogManager lm = LogManager.getInstance(); //로그 매니저 
	
		
		for (Tuple record : tuple) {
			
			//1. 새로운 tuple 만든 후, 트랜잭션 아이디 , 세션 값 추가 
			Tuple changeTuple = new Tuple(record.deepcopyValues(), trxId);
			changeTuple.setValue(value, attrIdxes[0]);
			changeTuple.setScn(Integer.MAX_VALUE);
			
			//2. 새로운 tuple 메모리에 추가 	
			int size = mtm.addTuple(changeTuple);
			changeTuple.rid = (RID)record.rid;
			changeTuple.rid.curOffset = size;
	
			//2. 원래 값은 ager manager 관리
			record.setSessionID(TransactionManager.getInstance().getSessionId(trxId));
			record.setNext(changeTuple);
			
			lm.addLogTuple(record, size);
		}
		
		if (tuple.size() == 1)
			return tuple.size() + " row updated. ";
		else
			return tuple.size() + " rows updated. ";
		
	}

	public String executeDelete(int sessionId) {
		PlanNode pn = qu.poll();

		int result = 0;
		if (pn != null) {
			result = pn.executePlanNode(dt,sessionId, viewSCN );
			while ((pn = qu.poll()) != null) {
				if (this.logical == 1) {
					String temp = pn.getAttr();
					for (int i = 0; i < tableColDic.size(); i++) { // attribute idx 찾기
						if (temp.equals(tableColDic.get(i).getColName())) {
							pn.attrIdx = i;
							break;
						}
					}
				} 
			}
		}

		if (result == 1)
			return result + " row deleted. ";
		else
			return result + " rows deleted. ";
	}

	public List<Tuple> byrowid(List<Tuple> executeOrder, int sessionId, int viewSCN) {
		MemoryTBSManager mtbsm = MemoryTBSManager.getInstance();
		MemoryTBS mtm = mtbsm.getMTM(dt);
		
		return mtm.visibleRID(executeOrder, sessionId, viewSCN);
	}
}
