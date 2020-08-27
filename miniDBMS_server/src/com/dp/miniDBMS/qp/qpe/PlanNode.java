package com.dp.miniDBMS.qp.qpe;

import java.util.List;

import com.dp.miniDBMS.sm.smo.DicTable;
import com.dp.miniDBMS.sm.smo.Tuple;

public abstract class PlanNode{
	public String attr = null;
	public int attrIdx = -1;
	
	Object liter = null;
	
	public PlanNode(String attr, Object liter) {
		this.attr = attr;
		this.liter = liter;
	}
	public String getAttr() {
		return attr;
	}
	public Object getLiter() {
		return liter;
	}
	
	public abstract int executePlanNode(DicTable dt, int sessionId, int viewSCN);
	public abstract List<Tuple> nextExecute();
	
	public abstract List<Tuple> fetchPlanNode(DicTable dt, int sessionId, int viewSCN );
	public abstract List<Tuple> nextFetch(DicTable dt,List<Tuple> data, int sessionId, int viewSCN);

}
