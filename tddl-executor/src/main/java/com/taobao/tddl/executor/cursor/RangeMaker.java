package com.taobao.tddl.executor.cursor;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.core.IRowSet;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;

public class RangeMaker {

    public static class Range {

        public IRowSet from;
        public IRowSet to;
    }

    public  RangeMaker.Range makeRange(IFilter lf, List<IOrderBy> orderBys) {
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

    public  ICursorMeta getICursorMetaByOrderBy(List<IOrderBy> orderBys) {
        List<ColumnMeta> columnsNew = new ArrayList<ColumnMeta>(orderBys.size());
        for (IOrderBy orderBy : orderBys) {
            ColumnMeta cmNew = GeneralUtil.getColumnMeta(orderBy.getColumn());
            columnsNew.add(cmNew);
        }

        ICursorMeta cursorMetaNew = CursorMetaImp.buildNew(columnsNew, orderBys.size());
        return cursorMetaNew;
    }

    private  void convertFilterToLowerAndTopLimit(RangeMaker.Range range, IFilter lf, ICursorMeta cursorMetaNew) {
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
    private  void processBoolfilter(RangeMaker.Range range, IFilter lf, ICursorMeta cursorMetaNew) {
        IBooleanFilter bf = (IBooleanFilter) lf;
        switch (bf.getOperation()) {
            case GT_EQ:
                setIntoRowSet(cursorMetaNew, bf, range.from, new ColumnEQProcessor() {

                    @Override
                    public Object process(Object c, DATA_TYPE type) {
                        return c;
                    }
                });
                break;
            case GT:
                setIntoRowSet(cursorMetaNew, bf, range.from, new ColumnEQProcessor() {

                    @Override
                    public Object process(Object c, DATA_TYPE type) {
                        return incr(c, type);
                    }
                });
                break;
            case LT:
                setIntoRowSet(cursorMetaNew, bf, range.to, new ColumnEQProcessor() {

                    @Override
                    public Object process(Object c, DATA_TYPE type) {
                        return decr(c, type);
                    }
                });
                break;
            case LT_EQ:
                setIntoRowSet(cursorMetaNew, bf, range.to, new ColumnEQProcessor() {

                    @Override
                    public Object process(Object c, DATA_TYPE type) {
                        return c;
                    }
                });
                break;
            case EQ:
                setIntoRowSet(cursorMetaNew, bf, range.from, new ColumnEQProcessor() {

                    @Override
                    public Object process(Object c, DATA_TYPE type) {
                        return c;
                    }
                });
                setIntoRowSet(cursorMetaNew, bf, range.to, new ColumnEQProcessor() {

                    @Override
                    public Object process(Object c, DATA_TYPE type) {
                        return c;
                    }
                });
                break;
            default:
                throw new IllegalArgumentException("not supported yet");
        }
    }

    private  void setIntoRowSet(ICursorMeta cursorMetaNew, IBooleanFilter bf, IRowSet rowSet,
                                      ColumnEQProcessor columnProcessor) {
        IColumn col = GeneralUtil.getIColumn(bf.getColumn());
        Object val = bf.getValue();
        if (col.getDataType() == DATA_TYPE.DATE_VAL) {
            if (val instanceof Long) {
                val = new Date((Long) val);
            }
        }
        val = processFunction(val);
        val = columnProcessor.process(val, col.getDataType());
        Integer inte = cursorMetaNew.getIndex(col.getTableName(), col.getColumnName());

        if (inte == null) inte = cursorMetaNew.getIndex(col.getTableName(), col.getAlias());
        rowSet.setObject(inte, val);
    }

    public  Object decr(Object c, DATA_TYPE type) {
        switch (type) {
            case INT_VAL:
                c = ((Integer) c) - 1;
                break;
            case LONG_VAL:
                c = ((Long) c) - 1;
                break;
            case SHORT_VAL:
                c = ((Long) c) - 1;
                break;
            case FLOAT_VAL:
                c = ((Long) c) - 0.000001f;
                break;
            case DOUBLE_VAL:
                c = ((Double) c) - 0.000001d;
                break;
            case DATE_VAL:
                c = new Date(((Date) c).getTime() - 1l);
                break;
            case TIMESTAMP:
                c = new Date(((Date) c).getTime() - 1l);
                break;
            case STRING_VAL:
                StringBuilder newStr = new StringBuilder();
                newStr.append(c);
                newStr.setCharAt(newStr.length() - 1, (char) (newStr.charAt(newStr.length() - 1) - 1));
                c = newStr.toString();
                break;
            default:
                throw new IllegalArgumentException("not supported yet");
        }
        return c;
    }

    /**
     * 用来做一些值的处理工作
     * 
     * @author whisper
     */
    public static interface ColumnEQProcessor {

        public Object process(Object c, DATA_TYPE type);
    }

    public  Object incr(Object c, DATA_TYPE type) {
        switch (type) {
            case INT_VAL: {
                if (c instanceof String) {// fix
                    c = Integer.valueOf(c.toString());
                }
                c = ((Integer) c) + 1;
                break;
            }

            case SHORT_VAL: {
                if (c instanceof String) {// fix
                    c = Short.valueOf(c.toString());
                }
                c = ((Short) c) + 1;
                break;
            }
            case LONG_VAL:
                c = ((Long) c) + 1;
                break;
            case FLOAT_VAL:
                c = ((Float) c) + 0.000001f;
                break;
            case DOUBLE_VAL:
                c = ((Double) c) + 0.000001d;
                break;
            case DATE_VAL:
                c = new Date(((Date) c).getTime() + 1l);
                break;
            case TIMESTAMP:
                c = new Date(((Date) c).getTime() + 1l);
                break;
            case STRING_VAL:
                StringBuilder newStr = new StringBuilder();
                newStr.append(c);
                newStr.setCharAt(newStr.length() - 1, (char) (newStr.charAt(newStr.length() - 1) + 1));
                c = newStr.toString();
                break;
            default:
                throw new IllegalArgumentException("not supported yet");
        }
        return c;
    }

    @SuppressWarnings("rawtypes")
    private  Comparable processFunction(Object v) {
        if (v instanceof IFunction) {/*
                                     * shenxun : 这个地方实现有点ugly。写个注释。。 函数在key
                                     * filter里面做起来难度较大，
                                     * 如果是个无参函数，或者函数内包含了所有列数据，比如pk = 1+1
                                     * 这样的表达式，那么是可以直接计算的。 但如果函数内包含当前keyFilter列本身
                                     * ，那么就需要遍历所有记录，进行条件判断。
                                     * 这个实现比较麻烦，意义不大，暂时不予实现。
                                     */

            IFunction func = (IFunction) v;
            func.getArgs();
            try {
                func.serverMap(null);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            Comparable val = (Comparable) func.getResult();
            return val;
        }
        return (Comparable) v;
    }

    private  void fillRowSet(IRowSet iRowSet, boolean max) {
        ICursorMeta icm = iRowSet.getParentCursorMeta();
        Iterator<ColMetaAndIndex> iterator = icm.indexIterator();
        while (iterator.hasNext()) {
            ColMetaAndIndex cai = iterator.next();
            String cname = cai.getName();
            List<ColumnMeta> columns = iRowSet.getParentCursorMeta().getColumns();
            // TODO shenxun : 如果条件是null。。。没处理。。应该在Comparator里面处理
            if (iRowSet.getObject(cai.getIndex()) == null) {
                boolean find = false;
                for (ColumnMessage cm : columns) {
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

    public  Object getExtremum(boolean max, DATA_TYPE type) {
        if (max) {
            switch (type) {
                case INT_VAL:
                    return Integer.MAX_VALUE;
                case LONG_VAL:
                    return Long.MAX_VALUE;
                case SHORT_VAL:
                    return Short.MAX_VALUE;
                case FLOAT_VAL:
                    return Float.MAX_VALUE;
                case DOUBLE_VAL:
                    return Double.MAX_VALUE;
                case DATE_VAL:
                    return new Date(Long.MAX_VALUE);
                case STRING_VAL:
                    return Character.MAX_VALUE + "";
                case TIMESTAMP:
                    return new Date(Long.MAX_VALUE);
                case BOOLEAN_VAL:
                    return new Boolean(true);
                case CHAR_VAL:
                    return Character.MAX_VALUE + "";
                default:
                    throw new IllegalArgumentException("not supported yet");
            }
        } else {
            // 任何类型的最小值都是null;
            return null;
        }
        // return null;
    }

}
