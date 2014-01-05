package com.taobao.tddl.executor.cursor.impl;

import java.util.Comparator;
import java.util.List;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.common.KVPair;
import com.taobao.tddl.executor.cursor.IRangeCursor;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.RangeMaker;
import com.taobao.tddl.executor.cursor.SchematicCursor;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * 现在具有对多个索引内前缀字段+非前缀字段
 * 
 * @author jianxing <jianxing.qx@taobao.com>
 */
public class RangeCursor extends SchematicCursor implements IRangeCursor {

    public static final Logger    log          = LoggerFactory.getLogger(RangeCursor.class);
    /**
     * 范围，到哪里结束
     */
    protected IRowSet             to;
    /**
     * 范围，从哪里开始？
     */
    protected IRowSet             from;

    protected Comparator<IRowSet> fromComparator;

    protected Comparator<IRowSet> toComparator;

    private boolean               schemaInited = false;

    private IFilter               lf           = null;

    boolean                       first        = true;

    public RangeCursor(ISchematicCursor cursor, IFilter lf){
        super(cursor, null, cursor.getOrderBy());
        this.lf = lf;
        RangeMaker.Range range = makeRange(lf, orderBys);
        this.from = range.from;
        this.to = range.to;
    }

    protected RangeMaker.Range makeRange(IFilter lf, List<IOrderBy> orderBys) {
        return new RangeMaker().makeRange(lf, orderBys);
    }

    private void initSchema(IRowSet firstRowSet, IFilter lf) {
        if (schemaInited) {
            return;
        }
        schemaInited = true;

        List<ColumnMeta> columnMetas = to.getParentCursorMeta().getColumns();
        List<ISelectable> iColumns = ExecUtils.getIColumnsWithISelectable(columnMetas.toArray(new ColumnMeta[0]));
        toComparator = ExecUtils.getComp(iColumns,
            iColumns,
            to.getParentCursorMeta(),
            firstRowSet.getParentCursorMeta());
        fromComparator = ExecUtils.getComp(iColumns,
            iColumns,
            from.getParentCursorMeta(),
            firstRowSet.getParentCursorMeta());
        // init(rangeFilters);
    }

    @Override
    public IRowSet next() throws TddlException {
        if (cursor == null) {
            return null;
        }
        IRowSet kv = null;

        if (!first) {
            kv = cursor.next();
        } else {
            first = false;
            kv = first();
            if (kv == null) {
                return null;
            }
        }
        if (kv == null) {
            return null;
        }

        if (match(kv)) {
            return kv;
        } else {
            return null;
        }

    }

    /**
     * @param kv
     * @return true 如果符合区间。false 如果不符合区间
     */
    private boolean match(IRowSet kv) {
        initSchema(kv, lf);
        int res = fromComparator.compare(from, kv);
        if (res == 0) {// eq;
            return true;
        } else if (res > 0) {
            return false;
        }

        res = toComparator.compare(to, kv);
        if (res == 0) {
            return true;
        } else if (res < 0) {
            return false;
        }
        return true;
    }

    @Override
    public boolean skipTo(CloneableRecord key) throws TddlException {
        if (parentCursorSkipTo(key)) {
            IRowSet kv = current();
            if (match(kv)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean skipTo(KVPair key) throws TddlException {
        return skipTo(key.getKey());
    }

    @Override
    public IRowSet first() throws TddlException {
        this.first = false;
        IRowSet iRowSet = from;
        CloneableRecord cr = ExecUtils.convertToClonableRecord(iRowSet);
        if (!cursor.skipTo(cr)) {
            return null;
        } else {
            return cursor.current();
        }
    }

    @Override
    public IRowSet last() throws TddlException {
        first = false;
        IRowSet iRowSet = to;
        CloneableRecord cr = ExecUtils.convertToClonableRecord(iRowSet);

        // to在数据中间有四种可能
        // data:5,6,7,8,9,10,15
        // case1: to是data的某个值，如9
        // case2: to大于最大值，如17
        // case3: to小于最小值，如2
        // case4: to在某两个值之间，如11
        // case5: data为空

        if (!cursor.skipTo(cr)) { // case2: to大于最大值，如17
            IRowSet last = cursor.last();
            if (last == null) {// case5: data为空
                return null;
            }
            if (match(last)) {
                // 只考虑to，是肯定匹配的，不匹配的可能是from
                return last;
            } else {
                return null;
            }
        } else {
            if (match(cursor.current())) { // case1: to是data的某个值，如9
                // 此时current和to相等
                return cursor.current();
            }// current>to或者current<from
             // 取prev的值
             // 如果current<from，则prev同样小于from，不会match
            else {

                IRowSet prevCurrent = cursor.prev();
                if (prevCurrent == null) { // case3: to小于最小值，如2
                    return null;
                } else { // case4: to在某两个值之间，如11
                    if (match(prevCurrent)) {
                        return prevCurrent;
                    } else {
                        return null;
                    }

                }
            }
        }
    }

    @Override
    public IRowSet prev() throws TddlException {
        IRowSet kv;

        if (!first) {
            kv = parentCursorPrev();
        } else {
            first = false;
            kv = last();
            if (kv == null) {
                return null;
            }
        }
        if (kv == null) {
            return null;
        }

        if (match(kv)) {
            return kv;
        } else {
            return null;
        }

    }

    @Override
    public String toString() {
        return toStringWithInden(0);
    }

    public String toStringWithInden(int inden) {
        StringBuilder sb = new StringBuilder();
        String tab = GeneralUtil.getTab(inden);
        sb.append(tab)
            .append("【Range cursor .start : ")
            .append(from)
            .append("|")
            .append(" end : ")
            .append(to)
            .append("\n");
        ExecUtils.printOrderBy(orderBys, inden, sb);
        sb.append(super.toStringWithInden(inden));
        return sb.toString();
    }

    @Override
    public void beforeFirst() throws TddlException {
        this.first = true;
    }

    public IRowSet getTo() {
        return to;
    }

    public void setTo(IRowSet to) {
        this.to = to;
    }

    public IRowSet getFrom() {
        return from;
    }

    public void setFrom(IRowSet from) {
        this.from = from;
    }

}
