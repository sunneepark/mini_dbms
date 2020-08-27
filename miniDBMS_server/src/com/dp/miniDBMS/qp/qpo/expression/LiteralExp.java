package com.dp.miniDBMS.qp.qpo.expression;

import java.util.List;

import com.dp.miniDBMS.qp.qpo.PlanTree;
import com.dp.miniDBMS.qp.qpp.SemanticException;
import com.dp.miniDBMS.qp.qpp.Token;
import com.dp.miniDBMS.sm.smo.DicColumn;
import com.dp.miniDBMS.sm.smo.type.TypeEnum;

/**
 * 
  *
  * @날짜 : Aug 10, 2020 
  * @작성자 : 박선희 
  * @클래스설명 : 문자열 토큰을 저장하는 클래스.
  *
 */
public class LiteralExp extends Exp {
	
	private Object value;
	
	public LiteralExp(Token tok, Object value) {
		super(tok);
		this.value = value;
	}
	
	public Object getValue() {
		return value;
	}
	
	public static String processStringLiteral(String literal) {
		StringBuilder sb = new StringBuilder(literal.length());
		char[] chars = literal.toCharArray();
		for (int i=1; i<chars.length-1; ++i) {	// ignore surrounding single quotes
			if (chars[i]=='\\' || chars[i]=='\'') {
				sb.append('\'');
				i++;	// skip next char
			}
			else {
				sb.append(chars[i]);
			}
		}
		return sb.toString();
	}

	@Override
	public Object visitor(PlanTree pt) throws SemanticException{
		if(pt.attrTemp != null) {
			DicColumn temp = pt.attrTemp;
			if (value instanceof Integer) {
				if(temp.getType().getType() != TypeEnum.INT)
					throw new SemanticException(temp.getColName() +"is INT!!");
			}
			else if (value instanceof String) {
				if(temp.getType().getType() != TypeEnum.CHAR)
					throw new SemanticException(temp.getColName() +"is CHAR!!");
				if (value.toString().length() > temp.getType().getMaxSize())
					throw new SemanticException(temp.getColName() + " should be less than " + temp.getType().getMaxSize());
			}
		}
		
		return value;
	}

}
