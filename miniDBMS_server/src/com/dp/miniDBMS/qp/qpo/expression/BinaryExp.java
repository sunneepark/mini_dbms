package com.dp.miniDBMS.qp.qpo.expression;

import com.dp.miniDBMS.qp.qpe.CmpPlan;
import com.dp.miniDBMS.qp.qpe.EqualPlan;
import com.dp.miniDBMS.qp.qpe.PlanNode;
import com.dp.miniDBMS.qp.qpo.*;
import com.dp.miniDBMS.qp.qpp.SemanticException;
import com.dp.miniDBMS.qp.qpp.Token;
import com.dp.miniDBMS.sm.smm.BPlusTree.RangePolicy;
import com.dp.miniDBMS.sm.smo.DicColumn;

/**
 * 
 *
 * @날짜 : Aug 8, 2020
 * @작성자 : 박선희
 * @클래스설명 : 부모 노드
 *
 */
public class BinaryExp extends Exp {
	Exp left, right;
	Token op;

	public BinaryExp(Exp left, Token op, Exp right) {
		super(op);
		this.left = left;
		this.op = op;
		this.right = right;
	}

	public Exp getLeft() {
		return left;
	}

	public Exp getRight() {
		return right;
	}

	public Token getOp() {
		return op;
	}

	public void setLeft(Exp left) {
		this.left = left;
	}

	@Override
	public Object visitor(PlanTree pt) throws SemanticException {
		System.out.println(op.image);
		if (op.image.equals("and")) {
			pt.setLogical(1);
			getLeft().visitor(pt);
		} else if (op.image.equals("or")) {
			pt.setLogical(0);
			getLeft().visitor(pt);
		}
		
		// 칼럼에 대한 서브 트리 일 때
		if (getLeft() instanceof AttributeExp) {
			DicColumn col = (DicColumn) getLeft().visitor(pt);
			PlanNode node = null;

			pt.attrTemp = col;
			Object liter = getRight().visitor(pt);
			
			if (op.image == "=") { // 등호 일 때
				node = new EqualPlan(col.getColName(), liter);
			} else { // 다른 비교 연산자 일 때
				switch (op.image) {
				case ">=":
					node = new CmpPlan(col.getColName(), liter, RangePolicy.INCLUSIVE, 1);
					break;
				case ">":
					node = new CmpPlan(col.getColName(), liter, RangePolicy.EXCLUSIVE, 1);
					break;
				case "<=":
					node = new CmpPlan(col.getColName(), liter, RangePolicy.INCLUSIVE, 0);
					break;
				case "<":
					node = new CmpPlan(col.getColName(), liter, RangePolicy.EXCLUSIVE, 0);
					break;
				}
			}

			pt.qu.add(node);
		}
		getRight().visitor(pt);
		
		// 계산에 대한 서브 트리 일 때
		if (getLeft() instanceof LiteralExp) {
			String whereCol = getLeft().visitor(pt).toString();

		}
		return null;
	}

}
