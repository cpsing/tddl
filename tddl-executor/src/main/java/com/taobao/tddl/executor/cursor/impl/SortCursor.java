package com.taobao.tddl.executor.cursor.impl;

import java.util.Comparator;
import java.util.List;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.SchematicCursor;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;

/**
 * 公共基类，用于排序
 * 
 * @author mengshi.sunmengshi
 * @author jianxing <jianxing.qx@taobao.com>
 */
public abstract class SortCursor extends SchematicCursor {

    /**
     * 比较器
     */
    protected Comparator<IRowSet> kvPairComparator;

    private boolean               schemaInited = false;

    public SortCursor(ISchematicCursor cursor, List<IOrderBy> orderBys) throws TddlException{
        super(cursor, null, orderBys);
    }

    protected void initComparator(List<IOrderBy> orderBys, ICursorMeta cursorMeta) {
        if (schemaInited) {
            return;
        }
        schemaInited = true;
        if (orderBys != null) {
            this.kvPairComparator = ExecUtils.getComp(orderBys, cursorMeta);
        }
    }
}
