package com.taobao.tddl.executor.cursor.impl;

import java.util.List;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.cursor.IReverseOrderCursor;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.SchematicCursor;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;

/**
 * 逆序遍历一个cursor
 * 
 * @author mengshi.sunmengshi 2013-12-3 上午10:57:45
 * @since 5.0.0
 */
public class ReverseOrderCursor extends SchematicCursor implements IReverseOrderCursor {

    public ReverseOrderCursor(ISchematicCursor cursor){
        super(cursor, null, cursor.getOrderBy());
        List<IOrderBy> orderByList = cursor.getOrderBy();
        reverseOrderBy(orderByList);
    }

    private void reverseOrderBy(List<IOrderBy> orderBy) {
        if (orderBy != null) {
            for (IOrderBy ob : orderBy) {
                if (ob.getDirection()) {
                    ob.setDirection(false);
                } else {
                    ob.setDirection(true);
                }
            }
        }
    }

    @Override
    public IRowSet next() throws TddlException {
        return parentCursorPrev();
    }

    @Override
    public String toString() {
        return toStringWithInden(0);
    }

    public String toStringWithInden(int inden) {
        StringBuilder sb = new StringBuilder();
        String tab = GeneralUtil.getTab(inden);
        sb.append(tab).append("【Reverse order cursor .").append("\n");
        ExecUtils.printOrderBy(orderBys, inden, sb);
        sb.append(super.toStringWithInden(inden));
        return sb.toString();
    }

}
