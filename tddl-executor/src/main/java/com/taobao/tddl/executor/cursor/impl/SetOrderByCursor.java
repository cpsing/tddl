package com.taobao.tddl.executor.cursor.impl;

import java.util.List;

import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.cursor.Cursor;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.cursor.ISetOrderCursor;
import com.taobao.tddl.executor.cursor.SchematicCursor;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;

public class SetOrderByCursor extends SchematicCursor implements ISetOrderCursor {

    public SetOrderByCursor(Cursor cursor, ICursorMeta meta, List<IOrderBy> orderBys){
        super(cursor, null, orderBys);
    }

    @Override
    public String toString() {
        return toStringWithInden(0);
    }

    public String toStringWithInden(int inden) {
        StringBuilder sb = new StringBuilder();
        String tab = GeneralUtil.getTab(inden);
        sb.append(tab).append("【set order cursor .").append("\n");
        ExecUtils.printOrderBy(orderBys, inden, sb);
        sb.append(super.toStringWithInden(inden));
        return sb.toString();
    }
}
