package com.taobao.tddl.executor.cursor.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.DuplicateKVPair;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.KVPair;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.IValueFilterCursor;
import com.taobao.tddl.executor.cursor.SchematicCursor;
import com.taobao.tddl.executor.function.ExtraFunction;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IFunction.FunctionType;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * 用于做没法走索引的条件过滤
 * 
 * @author mengshi.sunmengshi 2013-12-3 上午11:01:53
 * @since 5.0.0
 */
public class ValueFilterCursor extends SchematicCursor implements IValueFilterCursor {

    protected IFilter          filter;
    Pattern                    pattern;
    String                     tarCache;
    protected ExecutionContext executionContext;

    public ValueFilterCursor(ISchematicCursor cursor, IFilter filter, ExecutionContext executionContext){

        super(cursor, cursor == null ? null : null, cursor == null ? null : cursor.getOrderBy());
        // 将filter中的函数用规则引擎里的，带实际
        this.filter = filter;
        this.executionContext = executionContext;

    }

    DuplicateKVPair allow(IFilter f, DuplicateKVPair dkv) throws TddlException {
        if (f == null) {
            return dkv;
        }
        // 链表头，第一个allow的DKV放在这里，如果所有都不allow，那么这里为空
        DuplicateKVPair rootAllowDKV = null;
        // 链表尾，用于append新元素
        DuplicateKVPair rootAllowDKVTail = null;
        // 遍历用dkv
        DuplicateKVPair currentDKV = dkv;
        if (allowOneDKV(f, currentDKV)) {
            rootAllowDKV = currentDKV;
            rootAllowDKVTail = currentDKV;
        }
        while ((currentDKV = dkv.next) != null) {
            if (allowOneDKV(f, currentDKV)) {
                if (rootAllowDKV == null) {
                    // 如果这是第一个满足要求的DKV.设置tail和root
                    rootAllowDKV = currentDKV;
                    rootAllowDKVTail = currentDKV;
                } else {// 前面已经有满足要求的DVK了，设置tail.next并更新tail
                    rootAllowDKVTail.next = currentDKV;
                    rootAllowDKVTail = currentDKV;
                }
            }
        }
        if (rootAllowDKVTail != null && rootAllowDKVTail.next != null) {// 最后一个元素，可能有next，但next不满足要求
            rootAllowDKVTail.next = null;
        }
        return rootAllowDKV;
    }

    @Override
    public IRowSet next() throws TddlException {
        IRowSet kv = null;
        while ((kv = parentCursorNext()) != null) {
            if (allow(filter, kv)) {
                return kv;
            }
        }
        return null;
    }

    private boolean allowOneDKV(IFilter f, DuplicateKVPair dkv) throws TddlException {
        // 遍历链表，如果有notallow，就丢掉他。
        boolean ok = allow(f, dkv.currentKey);
        return ok;
    }

    @SuppressWarnings("unchecked")
    boolean allow(IFilter f, IRowSet iRowSet) throws TddlException {
        if (f == null) {
            return true;
        }
        if (f instanceof IBooleanFilter) {
            IBooleanFilter bf = (IBooleanFilter) f;
            Object column_value = null;
            Object col = bf.getColumn();
            if (col instanceof ISelectable) {
                try {

                    if (col instanceof IFunction && ((IFunction) col).getFunctionType().equals(FunctionType.Scalar)) {
                        column_value = processFunction(iRowSet, col);

                    } else {
                        // TODO shenxun : 这是否应该用cursorMeta?
                        column_value = ExecUtils.getValueByIColumn(iRowSet, (ISelectable) col);
                    }
                } catch (Exception e) {
                    throw new TddlException(e);
                }
            } else {
                throw new IllegalArgumentException("" + "暂时不支持左值为非IExpression");
            }
            // if (column_value instanceof Utf8) {
            // column_value = column_value.toString();
            // }
            Object v = null;
            List values = bf.getValues();
            if (bf.getValue() instanceof IColumn) {
                IColumn c = (IColumn) bf.getValue();
                if (c instanceof ISelectable) {
                    try {
                        if (c instanceof IFunction) {
                            {
                                v = processFunction(iRowSet, c);
                            }
                        } else {
                            v = ExecUtils.getValueByIColumn(iRowSet, (IColumn) col);
                        }
                    } catch (Exception e) {
                        throw new TddlException(e);
                    }
                } else {
                    throw new IllegalArgumentException("" + "暂时不支持左值为非IExpression");
                }
            } else {
                v = bf.getValue();
            }
            // if (v instanceof Utf8) {
            // v = v.toString();
            // }
            // shenxun bug fix : 这里未考虑参数为null的情况
            if (v == null && values == null) {

                if ((bf.getOperation() == OPERATION.EQ || bf.getOperation() == OPERATION.IS_NULL)) {
                    if (column_value == null) {
                        return true;
                    }
                } else if (bf.getOperation() == OPERATION.NOT_EQ || bf.getOperation() == OPERATION.IS_NOT_NULL) {
                    if (column_value != null) {
                        return true;
                    }
                }
                // 其他情况，比如> < >= <= ....
                return false;
            }
            if (column_value == null) {
                // shenxun:前面已经判断过，v不为空了，走到这里的话就是v不为空，但当前列为空，那么应该返回false;
                return false;
            }
            if (v instanceof IFunction) {
                try {
                    if (((IFunction) v).getFunctionType().equals(FunctionType.Aggregate)) {
                        throw new RuntimeException("Invalid use of group function");
                    }

                    ((ExtraFunction) ((IFunction) v).getExtraFunction()).serverMap(iRowSet);
                    v = processFunction(iRowSet, v);
                } catch (Exception e) {
                    throw new TddlException(e);
                }
            }
            OPERATION op = bf.getOperation();
            if (op == OPERATION.LIKE) {
                return processLike(column_value, v);
            }

            if (op == OPERATION.IN) {
                return processIn(column_value, bf.getValues());
            }

            int n = ((Comparable) v).compareTo(column_value);

            if (n == 0) {
                if (op == OPERATION.EQ || op == OPERATION.GT_EQ || op == OPERATION.LT_EQ) {
                    return true;
                }
            } else if (n < 0) {
                if (op == OPERATION.GT || op == OPERATION.GT_EQ || op == OPERATION.NOT_EQ) {
                    return true;
                }
            } else {
                if (op == OPERATION.LT || op == OPERATION.LT_EQ || op == OPERATION.NOT_EQ) {
                    return true;
                }
            }
        } else if (f instanceof ILogicalFilter) {
            ILogicalFilter lf = (ILogicalFilter) f;
            if (f.getOperation().equals(OPERATION.AND)) {
                for (IFilter f1 : lf.getSubFilter()) {
                    if (!allow(f1, iRowSet)) {
                        return false;
                    }
                }
            } else if (f.getOperation().equals(OPERATION.OR)) {// shenxun :
                // 增加一个or条件全部匹配的逻辑
                for (IFilter f1 : lf.getSubFilter()) {
                    if (allow(f1, iRowSet)) {
                        return true;
                    }
                }
                return false;
            } else {
                throw new IllegalArgumentException("should not be here ");
            }
            return true;
        }
        return false;
    }

    private Object processFunction(IRowSet iRowSet, Object c) throws TddlException {
        Object v = null;
        // 在Filter里面是不能出现聚合函数的
        if (((IFunction) c).getFunctionType().equals(FunctionType.Aggregate)) throw new RuntimeException("Invalid use of group function");
        ((ExtraFunction) ((IFunction) c).getExtraFunction()).serverMap(iRowSet);

        v = ((IFunction) c).getExtraFunction().getResult();
        return v;
    }

    private boolean processIn(Object column_value, List<Object> values) {

        if (values.contains(column_value)) return true;

        return false;
    }

    @Override
    public boolean skipTo(CloneableRecord key) throws TddlException {
        if (super.skipTo(key)) {
            IRowSet kv = parentCursorCurrent();
            if (kv != null && allow(filter, kv)) {
                return true;
            }
        }
        return false;
    }

    protected boolean processLike(Object column_value, Object v) {
        if (!(v == null || v instanceof String)) {// TODO shenxun
            // 丢异常才对，但老实现丢异常会出现无法关闭cursor的问题。所以返回false.
            return false;
        }
        String colValString = "";
        if (column_value != null) {
            colValString = String.valueOf(column_value);
        }

        String tar = (String) v;
        if (tarCache == null || !tarCache.equals(tar)) {
            if (pattern != null) {
                throw new IllegalArgumentException("should not be here");
            }

            tarCache = tar;
            // trim and remove %%
            tar = TStringUtil.trim(tar);
            tar = TStringUtil.replace(tar, "\\_", "[uANDOR]");
            tar = TStringUtil.replace(tar, "\\%", "[pANDOR]");
            tar = TStringUtil.replace(tar, "%", ".*");
            tar = TStringUtil.replace(tar, "_", ".");
            tar = TStringUtil.replace(tar, "[uANDOR]", "\\_");
            tar = TStringUtil.replace(tar, "[pANDOR]", "\\%");

            // case insensitive
            tar = "(?i)" + tar;

            tar = "^" + tar;
            tar = tar + "$";

            pattern = Pattern.compile(tar);

        }
        Matcher matcher = pattern.matcher(colValString);
        return matcher.find();
    }

    @Override
    public boolean skipTo(KVPair key) throws TddlException {
        return skipTo(key.getKey());
    }

    @Override
    public IRowSet first() throws TddlException {
        IRowSet kv = parentCursorFirst();
        if (kv != null) {
            do {
                if (kv != null && allow(filter, kv)) {
                    return kv;
                }
            } while ((kv = parentCursorNext()) != null);
        }
        return null;
    }

    // @Override
    // public KVPair get(KVPair key) throws Exception {
    // return get(key.getKey());
    // }
    //
    // @Override
    // public KVPair get(CloneableRecord key) throws Exception {
    // KVPair kv = super.get(key);
    // return kv!=null && allow(filter, kv.getKey(), kv.getValue())?kv:null;
    // }

    @Override
    public IRowSet last() throws TddlException {
        IRowSet kv = parentCursorLast();
        do {
            if (kv != null && allow(filter, kv)) {
                return kv;
            }
        } while ((kv = parentCursorPrev()) != null);
        return null;
    }

    @Override
    public IRowSet prev() throws TddlException {
        IRowSet kv = parentCursorPrev();
        do {
            if (kv != null && allow(filter, kv)) {
                return kv;
            }
        } while ((kv = parentCursorPrev()) != null);
        return null;
    }

    @SuppressWarnings("unused")
    private Map<String, Object> getRecordMap(CloneableRecord key, CloneableRecord value) {
        int size = 0;
        if (value != null) {
            size += value.getColumnList().size();
        }
        if (key != null) {
            size += key.getColumnList().size();
        }
        Map<String, Object> eachRow = new HashMap<String, Object>(size);

        if (value != null) {
            eachRow.putAll(value.getMap());
        }
        if (key != null) {
            eachRow.putAll(key.getMap());
        }
        return eachRow;
    }

    @Override
    public Map<CloneableRecord, DuplicateKVPair> mgetWithDuplicate(List<CloneableRecord> keys, boolean prefixMatch,
                                                                   boolean keyFilterOrValueFilter) throws TddlException {
        Map<CloneableRecord, DuplicateKVPair> map = super.mgetWithDuplicate(keys, prefixMatch, keyFilterOrValueFilter);
        if (map == null) {
            return null;
        }
        Map<CloneableRecord, DuplicateKVPair> retMap = new HashMap<CloneableRecord, DuplicateKVPair>(map.size());
        Iterator<Entry<CloneableRecord, DuplicateKVPair>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<CloneableRecord, DuplicateKVPair> entry = iterator.next();
            DuplicateKVPair dkv = entry.getValue();
            dkv = allow(filter, dkv);
            if (dkv != null) {
                retMap.put(entry.getKey(), dkv);
            }
        }
        return retMap;
    }

    @Override
    public String toStringWithInden(int inden) {
        StringBuilder sb = new StringBuilder();
        String subQueryTab = GeneralUtil.getTab(inden);
        sb.append(subQueryTab).append("【Value Filter Cursor : ").append("\n");
        sb.append(subQueryTab).append(filter).append("\n");
        ExecUtils.printOrderBy(orderBys, inden, sb);
        sb.append(super.toStringWithInden(inden));
        return sb.toString();
    }

    @Override
    public String toString() {
        return toStringWithInden(0);
    }
}
