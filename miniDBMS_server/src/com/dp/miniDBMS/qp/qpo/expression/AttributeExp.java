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
  * @클래스설명 : 칼럼에 관한 토큰 저장.
  *
 */
public class AttributeExp extends Exp {
    String name;

    public AttributeExp(Token tok, String name) {
        super(tok);
        this.name = name;
    }

    public String getName() { return name; }


	@Override
	public Object visitor(PlanTree pt) throws SemanticException {
		int i=0;
		List<DicColumn> dt = pt.tableColDic;
		//column validation
		for(;i<dt.size();i++) {
			if (name.equals(dt.get(i).getColName()))
				break;
		}
		if(i >= dt.size())
			throw new SemanticException("there is no column "+name); 
		return dt.get(i);
	}
}
