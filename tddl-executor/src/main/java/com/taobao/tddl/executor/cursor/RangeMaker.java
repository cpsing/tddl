package com.taobao.tddl.executor.cursor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.taobao.tddl.executor.cursor.impl.ColMetaAndIndex;
import com.taobao.tddl.executor.cursor.impl.CursorMetaImp;
import com.taobao.tddl.executor.function.ExtraFunction;
import com.taobao.tddl.executor.rowset.ArrayRowSet;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DataTypeUtil;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;

public class RangeMaker {

    public static class Range {

        public IRowSet from;
        public IRowSet to;
    }

    public RangeMaker.Range makeRange(IFilter lf, List<IOrderBy> orderBys) {
        RangeMaker.Range range = new RangeMaker.Range();

        ICursorMeta cursorMetaNew = getICursorMetaByOrderBy(orderBys);
        range.to = new ArrayRowSet(orderBys.size(), cursorMetaNew);
        //
        range.from = new ArrayRowSet(orderBys.size(), cursorMetaNew);

        convertFilterToLowerAndTopLimit(range, lf, cursorMetaNew);

        boolean min = false;
        fillRowSet(range.from, min);
        boolean max = true;
        fillRowSet(range.to, max);
        return range;
    }

    public ICursorMeta getICursorMetaByOrderBy(List<IOrderBy> orderBys) {
        List<ColumnMeta> columnsNew = new ArrayList<ColumnMeta>(orderBys.size());
        for (IOrderBy orderBy : orderBys) {
            ColumnMeta cmNew = ExecUtils.getColumnMeta(orderBy.getColumn());
            columnsNew.add(cmNew);
        }

        ICursorMeta cursorMetaNew = CursorMetaImp.buildNew(columnsNew, orderBys.size());
        return cursorMetaNew;
    }

    private void convertFilterToLowerAndTopLimit(RangeMaker.Range range, IFilter lf, ICursorMeta cursorMetaNew) {
        if (lf instanceof IBooleanFilter) {
            processBoolfilter(range, lf, cursorMetaNew);
        } else if (lf instanceof ILogicalFilter) {
            ILogicalFilter lo = (ILogicalFilter) lf;
            if (lo.getOperation() == OPERATION.OR) {
                throw new IllegalStateException("or ? should not be here");
            }
            List<IFilter> list = lo.getSubFilter();
            for (IFilter filter : list) {
                convertFilterToLowerAndTopLimit(range, filter, cursorMetaNew);
            }
        }
    }

    /**
     * 将大于等于 变成大于 小于 变成小于等于 将
     * 
     * @param lf
     * @param cursorMetaNew
     */
    private void processBoolfilter(RangeMaker.Range range, IFilter lf, ICursorMeta cursorMetaNew) {
        IBooleanFilter bf = (IBooleanFilter) lf;
        switch (bf.getOperation()) {
            case GT_EQ:
                setIntoRowSet(cursorMetaNew, bf, range.from, new ColumnEQProcessor() {

                    @Override
                    public Object process(Object c, DataType type) {
                        return c;
                    }
                });
                break;
            case GT:
                setIntoRowSet(cursorMetaNew, bf, range.from, new ColumnEQProcessor() {

                    @Override
                    public Object process(Object c, DataType type) {
                        return incr(c, type);
                    }
                });
                break;
            case LT:
                setIntoRowSet(cursorMetaNew, bf, range.to, new ColumnEQProcessor() {

                    @Override
                    public Object process(Object c, DataType type) {
                        return decr(c, type);
                    }
                });
                break;
            case LT_EQ:
                setIntoRowSet(cursorMetaNew, bf, range.to, new ColumnEQProcessor() {

                    @Override
                    public Object process(Object c, DataType type) {
                        return c;
                    }
                });
                break;
            case EQ:
                setIntoRowSet(cursorMetaNew, bf, range.from, new ColumnEQProcessor() {

                    @Override
                    public Object process(Object c, DataType type) {
                        return c;
                    }
                });
                setIntoRowSet(cursorMetaNew, bf, range.to, new ColumnEQProcessor() {

                    @Override
                    public Object process(Object c, DataType type) {
                        return c;
                    }
                });
                break;
            default:
                throw new IllegalArgumentException("not supported yet");
        }
    }

    private void setIntoRowSet(ICursorMeta cursorMetaNew, IBooleanFilter bf, IRowSet rowSet,
                               ColumnEQProcessor columnProcessor) {
        IColumn col = ExecUtils.getIColumn(bf.getColumn());
        Object val = bf.getValue();
        // if (col.getDataType() == DATA_TYPE.DATE_VAL) {
        // if (val instanceof Long) {
        // val = new Date((Long) val);
        // }
        // }
        val = processFunction(val);
        val = columnProcessor.process(val, col.getDataType());
        Integer inte = cursorMetaNew.getIndex(col.getTableName(), col.getColumnName());

        if (inte == null) inte = cursorMetaNew.getIndex(col.getTableName(), col.getAlias());
        rowSet.setObject(inte, val);
    }

    public Object decr(Object c, DataType type) {
        if (type == null) {
            type = DataTypeUtil.getTypeOfObject(c); // 可能为null
        }
        return type.decr(c);
    }

    /**
     * 用来做一些值的处理工作
     * 
     * @author whisper
     */
    public static interface ColumnEQProcessor {

        public Object process(Object c, DataType type);
    }

    public Object incr(Object c, DataType type) {
        if (type == null) {
            type = DataTypeUtil.getTypeOfObject(c); // 可能为null
        }

        return type.incr(c);
    }

    @SuppressWarnings("rawtypes")
    private Comparable processFunction(Object v) {
        if (v instanceof IFunction) {/*
                                      * shenxun : 这个地方实现有点ugly。写个注释。。 函数在key
                                      * filter里面做起来难度较大，
                                      * 如果是个无参函数，或者函数内包含了所有列数据，比如pk = 1+1
                                      * 这样的表达式，那么是可以直接计算的。
                                      * 但如果函数内包含当前keyFilter列本身
                                      * ，那么就需要遍历所有记录，进行条件判断。
                                      * 这个实现比较麻烦，意义不大，暂时不予实现。
                                      */

            IFunction func = (IFunction) v;
            func.getArgs();
            try {
                ((ExtraFunction) func.getExtraFunction()).serverMap((IRowSet) null);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            Comparable val = (Comparable) func.getExtraFunction().getResult();
            return val;
        }
        return (Comparable) v;
    }

    private void fillRowSet(IRowSet iRowSet, boolean max) {
        ICursorMeta icm = iRowSet.getParentCursorMeta();
        Iterator<ColMetaAndIndex> iterator = icm.indexIterator();
        while (iterator.hasNext()) {
            ColMetaAndIndex cai = iterator.next();
            String cname = cai.getName();
            List<ColumnMeta> columns = iRowSet.getParentCursorMeta().getColumns();
            // TODO shenxun : 如果条件是null。。。没处理。。应该在Comparator里面处理
            if (iRowSet.getObject(cai.getIndex()) == null) {
                boolean find = false;
                for (ColumnMeta cm : columns) {
                    if (cname.equalsIgnoreCase(cm.getName())) {
                        find = true;
                        iRowSet.setObject(cai.getIndex(), getExtremum(max, cm.getDataType()));
                        break;
                    }
                }
                if (!find) {
                    throw new IllegalStateException(" can't find column name : " + cname + " on . " + columns);
                }
            }
        }
    }

    public Object getExtremum(boolean max, DataType type) {
        if (max) {

            return type.getMaxValue();
            // switch (type) {
            // case INT_VAL:
            // return Integer.MAX_VALUE;
            // case LONG_VAL:
            // return Long.MAX_VALUE;
            // case SHORT_VAL:
            // return Short.MAX_VALUE;
            // case FLOAT_VAL:
            // return Float.MAX_VALUE;
            // case DOUBLE_VAL:
            // return Double.MAX_VALUE;
            // case DATE_VAL:
            // return new Date(Long.MAX_VALUE);
            // case STRING_VAL:
            // return Character.MAX_VALUE + "";
            // case TIMESTAMP_VAL:
            // return new Date(Long.MAX_VALUE);
            // case BOOLEAN_VAL:
            // return new Boolean(true);
            // case CHAR_VAL:
            // return Character.MAX_VALUE + "";
            // default:
            // throw new IllegalArgumentException("not supported yet");
            // }
        } else {
            // 任何类型的最小值都是null;
            return type.getMinValue();
        }
        // return null;
    }

}
