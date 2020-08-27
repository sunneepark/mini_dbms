package com.dp.miniDBMS.qp.qpe;

import java.util.List;
import java.util.stream.Collectors;

import com.dp.miniDBMS.cm.TransactionManager;
import com.dp.miniDBMS.sm.smm.MemoryTBS;
import com.dp.miniDBMS.sm.smm.MemoryTBSManager;
import com.dp.miniDBMS.sm.smm.BPlusTree.RangePolicy;
import com.dp.miniDBMS.sm.smo.DicTable;
import com.dp.miniDBMS.sm.smo.Tuple;

public class CmpPlan extends PlanNode {
	RangePolicy policy = null;
	private int type = -1; // 0 : literal 보다 작은 , 1: literal 보다 큰

	public CmpPlan(String attr, Object liter, RangePolicy policy, int type) {
		super(attr, liter);
		this.policy = policy;
		this.type = type;
	}


	@Override
	public int executePlanNode(DicTable dt, int sessionId, int viewSCN) {
		MemoryTBSManager mtbsm = MemoryTBSManager.getInstance();
		MemoryTBS mtm = mtbsm.getMTM(dt);
		
		List<Tuple> temp = null;
		if (type == 0)
			temp = mtm.doFullScanRange(attr, Integer.MIN_VALUE, (Integer) liter, policy);
		else if (type == 1)
			temp = mtm.doFullScanRange(attr, (Integer) liter, Integer.MAX_VALUE, policy);

		return mtm.delete(mtm.visibleRID(temp, sessionId, viewSCN), sessionId);
	}
	@Override
	public List<Tuple> fetchPlanNode(DicTable dt, int sessionId, int viewSCN) {
		MemoryTBSManager mtbsm = MemoryTBSManager.getInstance();
		MemoryTBS mtm = mtbsm.getMTM(dt);
		
		List<Tuple> temp = null;
		if (type == 0)
			temp =  mtm.doWhereRange(attr, Integer.MIN_VALUE, (Integer) liter, policy, sessionId, viewSCN);
		else if (type == 1)
			temp = mtm.doWhereRange(attr, (Integer) liter, Integer.MAX_VALUE, policy, sessionId, viewSCN);

		return temp;
	}

	@Override
	public List<Tuple> nextFetch(DicTable dt, List<Tuple> data, int sessionId, int viewSCN) {
		TransactionManager.getInstance().setExplain(sessionId, "full scan\n");
		
		if(type == 0) {
			if(policy == RangePolicy.EXCLUSIVE) {
				return (List<Tuple>) data.stream()
						.filter(xx -> Integer.parseInt(xx.getValues().get(attrIdx).getValue().toString()) < (Integer) liter)
						.collect(Collectors.toList());
			}
			else if(policy == RangePolicy.INCLUSIVE){
				return (List<Tuple>) data.stream()
						.filter(xx -> Integer.parseInt(xx.getValues().get(attrIdx).getValue().toString()) <= (Integer) liter)
						.collect(Collectors.toList());
			}
		}
		else if(type == 1) {
			if(policy == RangePolicy.EXCLUSIVE) {
				return (List<Tuple>) data.stream()
						.filter(xx -> Integer.parseInt(xx.getValues().get(attrIdx).getValue().toString()) > (Integer) liter)
						.collect(Collectors.toList());
			}
			else if(policy == RangePolicy.INCLUSIVE){
				return (List<Tuple>) data.stream()
						.filter(xx -> Integer.parseInt(xx.getValues().get(attrIdx).getValue().toString()) >= (Integer) liter)
						.collect(Collectors.toList());
			}
		}
		return null;
	}
	@Override
	public List<Tuple> nextExecute() {
		return null;
	}



}
