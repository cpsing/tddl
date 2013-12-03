package com.taobao.ustore.spi.cursor.common;

import java.util.Comparator;
import java.util.List;

import com.taobao.tddl.executor.common.ICursorMeta;
import com.taobao.tddl.executor.common.KVPair;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.SchematicCursor;
import com.taobao.tddl.executor.rowset.IRowSet;
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
    protected KVPair              tmp;
    /**
     * 
     */
    protected KVPair              prev;

    private boolean               schemaInited = false;

    public SortCursor(ISchematicCursor cursor, List<IOrderBy> orderBys) throws FetchException{
        super(cursor, null, orderBys);
    }

    protected void initComparator(List<IOrderBy> orderBys, ICursorMeta cursorMeta) {
        if (schemaInited) {
            return;
        }
        schemaInited = true;
        if (orderBys != null) {
            this.kvPairComparator = ExecUtil.getComp(orderBys, cursorMeta);
        }
    }
}
