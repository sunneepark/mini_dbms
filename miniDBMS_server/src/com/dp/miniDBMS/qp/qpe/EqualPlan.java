package com.dp.miniDBMS.qp.qpe;

import java.util.List;
import java.util.stream.Collectors;

import com.dp.miniDBMS.cm.TransactionManager;
import com.dp.miniDBMS.sm.smm.MemoryTBS;
import com.dp.miniDBMS.sm.smm.MemoryTBSManager;
import com.dp.miniDBMS.sm.smo.DicTable;
import com.dp.miniDBMS.sm.smo.Tuple;

public class EqualPlan extends PlanNode {

	public EqualPlan(String attr, Object liter) {
		super(attr, liter);
	}

	@Override
	public int executePlanNode(DicTable dt, int sessionId, int viewSCN) {
		MemoryTBSManager mtbsm = MemoryTBSManager.getInstance();
		MemoryTBS mtm = mtbsm.getMTM(dt);

		return mtm.delete(mtm.visibleRID(mtm.doFullScan(attr, (Integer) liter), sessionId, viewSCN), sessionId);
	}
	@Override
	public List<Tuple> nextExecute() {
		return null;
	}

	@Override
	public List<Tuple> fetchPlanNode(DicTable dt, int sessionId, int viewSCN) {
		MemoryTBSManager mtbsm = MemoryTBSManager.getInstance();
		MemoryTBS mtm = mtbsm.getMTM(dt);

		List<Tuple> temp = mtm.doWhere(attr, (Integer) liter, sessionId, viewSCN);
		return temp;
	}

	@Override
	public List<Tuple> nextFetch(DicTable dt, List<Tuple> data, int sessionId, int viewSCN) {
		TransactionManager.getInstance().setExplain(sessionId, "full scan\n");
		return (List<Tuple>) data.stream()
				.filter(xx -> Integer.parseInt(xx.getValues().get(attrIdx).getValue().toString()) == (Integer) liter)
				.collect(Collectors.toList());
	}
}