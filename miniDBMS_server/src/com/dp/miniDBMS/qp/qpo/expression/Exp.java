package com.dp.miniDBMS.qp.qpo.expression;

import java.util.List;

import com.dp.miniDBMS.qp.qpo.PlanTree;
import com.dp.miniDBMS.qp.qpp.SemanticException;
import com.dp.miniDBMS.qp.qpp.Token;
import com.dp.miniDBMS.sm.smo.DicColumn;
/**
 * 
  *
  * @날짜 : Aug 8, 2020 
  * @작성자 : 박선희 
  * @클래스설명 : 모든 표현식의 기본
  *
 */
public abstract class Exp {
    private Token tok;
	private static StringBuilder sb = new StringBuilder();
	private String expString = null; //전체 표현식
	
	public Exp(Token tok) {
		this.tok = tok;
	}
   
	public static void clearGlobalExpString() {
		sb.setLength(0);
	}
	
	public static void appendToGlobalExpString(char c) {
		sb.append(c);
	}
	public static void appendToGlobalExpString(String s) {
		sb.append(s);
	}
	
	public void saveExpString() {
		expString = sb.toString();
	}
	
	public String getExpString() {
		return expString;
	}
	public abstract Object visitor(PlanTree pt) throws SemanticException;
}
