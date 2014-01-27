package com.taobao.tddl.executor.cursor.impl;

import java.util.List;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;

/**
 * 去重操作
 * 
 * @author mengshi.sunmengshi 2013-12-3 上午10:54:54
 * @since 5.0.0
 */
public class DistinctCursor extends MergeSortedCursors {

    public DistinctCursor(ISchematicCursor cursor, List<IOrderBy> distinctOrderbys) throws TddlException{
        super(cursor, false);
        this.cursor = cursor;
        this.orderBys = distinctOrderbys;
    }

    @Override
    public String toStringWithInden(int inden) {
        String tabTittle = GeneralUtil.getTab(inden);
        String tabContent = GeneralUtil.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();
        GeneralUtil.printlnToStringBuilder(sb, tabTittle + "DistinctCursor ");
        GeneralUtil.printAFieldToStringBuilder(sb, "orderBy", this.orderBys, tabContent);
        sb.append(tabContent).append("cursor:").append("\n");
        sb.append(cursor.toStringWithInden(inden + 1));
        return sb.toString();
    }

    @Override
    public IRowSet next() throws TddlException {
        IRowSet next = null;
        while ((next = (cursor.next())) != null) {
            if (current == null) {
                break;
            }
            super.initComparator(orderBys, next.getParentCursorMeta());
            int n = kvPairComparator.compare(next, current);
            if (n != 0) {
                break;
            }
            next = null;
        }
        this.current = ExecUtils.fromIRowSetToArrayRowSet(next);
        return next;
    }

}
