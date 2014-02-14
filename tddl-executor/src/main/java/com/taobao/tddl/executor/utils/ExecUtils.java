package com.taobao.tddl.executor.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.IRowsValueScaner;
import com.taobao.tddl.executor.common.KVPair;
import com.taobao.tddl.executor.common.RowsValueScanerImp;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.cursor.impl.ColMetaAndIndex;
import com.taobao.tddl.executor.cursor.impl.CursorMetaImp;
import com.taobao.tddl.executor.record.CloneableRecord;
import com.taobao.tddl.executor.record.FixedLengthRecord;
import com.taobao.tddl.executor.rowset.ArrayRowSet;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.rowset.ResultSetRowSet;
import com.taobao.tddl.executor.rowset.RowSetWrapper;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DataTypeUtil;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.expression.bean.OrderBy;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;

public class ExecUtils {

    static Pattern pattern = Pattern.compile("\\d+$");

    public static void printMeta(ICursorMeta meta, int inden, StringBuilder sb) {
        if (meta != null) {
            sb.append(meta.toStringWithInden(inden));
            sb.append("\n");
        }
    }

    public static void printMeta(IndexMeta meta, int inden, StringBuilder sb) {
        if (meta != null) {
            sb.append(meta.toStringWithInden(inden));
            sb.append("\n");
        }
    }

    public static void printOrderBy(List<IOrderBy> orderBys, int inden, StringBuilder sb) {
        if (orderBys != null) {
            String tab = GeneralUtil.getTab(inden);
            sb.append(tab).append("order by : ");
            for (IOrderBy orderBy : orderBys) {
                sb.append(orderBy.toStringWithInden(inden + 1)).append(" ");
            }
            sb.append("\n");
        }
    }

    public static KVPair rowSetTOKVPair(IRowSet rowSet, IndexMeta meta) {
        throw new RuntimeException("增加 IRowSet到 KVPair的实现");
    }

    public static Date convertStringToDate(String s) {
        try {
            SimpleDateFormat dateformat1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date d;
            d = dateformat1.parse(s);
            return d;
        } catch (ParseException e) {

        }

        try {
            SimpleDateFormat dateformat1 = new SimpleDateFormat("yyyy-MM-dd");
            Date d;
            d = dateformat1.parse(s);
            return d;
        } catch (ParseException e) {
        }

        throw new RuntimeException("Date format:" + s + " is not supported");
    }

    public static IRowSet fromIRowSetToArrayRowSet(IRowSet rsrs) {
        // 初衷是为了让mysql的代理换成实际数据
        if (rsrs == null) {
            return null;
        }
        if (rsrs instanceof ResultSetRowSet) {
            List<Object> values = rsrs.getValues();
            IRowSet rs = new ArrayRowSet(rsrs.getParentCursorMeta(), values.toArray());
            return rs;
        } else if (rsrs instanceof RowSetWrapper) {
            return IRowSetWrapperToArrayRowSet((RowSetWrapper) rsrs);
        } else {
            return rsrs;
        }
    }

    public static Object get(ColumnMeta c, CloneableRecord r) {
        return r.get(c.getTableName(), c.getName());
    }

    public static Object get(IColumn c, CloneableRecord r) {
        return r.get(c.getTableName(), c.getColumnName());
    }

    public static String getColumFullName(Object column) {
        if (column instanceof IColumn) {
            IColumn col = getIColumn(column);
            String colName = col.getTableName() + "." + col.getColumnName();
            if (col.getAlias() != null) {
                return colName + " as " + col.getAlias();
            } else {
                return colName;
            }

        } else if (column instanceof IFunction && !(column instanceof IFilter)) {
            IFunction func = ((IFunction) column);
            StringBuilder sb = new StringBuilder();
            sb.append(func.getFunctionName());
            sb.append("(");
            boolean first = true;
            if (func.isDistinct()) {
                sb.append("DISTINCT ");
            }
            for (Object arg : func.getArgs()) {
                if (first) {
                    first = false;
                } else {
                    sb.append(",");
                }
                sb.append(getColumnName(arg));
            }
            sb.append(")");

            if (func.getAlias() != null) {
                sb.append(" as ").append(func.getAlias());
            }
            return sb.toString();
        } else {
            return String.valueOf(column);
        }
    }

    public static IColumn getColumn(Object column) {
        return getIColumn(column);
    }

    public static ColumnMeta getColumnMeta(Object o) {
        ISelectable c = (ISelectable) o;
        return new ColumnMeta(c.getTableName(), c.getColumnName(), c.getDataType(), c.getAlias(), true);
    }

    public static ColumnMeta getColumnMeta(Object o, String columnAlias) {
        ISelectable c = (ISelectable) o;
        return new ColumnMeta(c.getTableName(), columnAlias, c.getDataType(), c.getAlias(), true);
    }

    public static ColumnMeta getColumnMeta(Object o, String tableName, String columnAlias) {
        ISelectable c = (ISelectable) o;
        return new ColumnMeta(tableName, columnAlias, c.getDataType(), c.getAlias(), true);
    }

    public static ColumnMeta getColumnMetaTable(Object o, String tableName) {
        ISelectable c = (ISelectable) o;
        return new ColumnMeta(tableName, c.getColumnName(), c.getDataType(), c.getAlias(), true);
    }

    public static String getColumnName(int index, String columns) {
        String[] strs = columns.split(",");
        if (index >= strs.length) {
            return "error column name";
        }
        return strs[index];

    }

    public static String getColumnName(Object column) {
        if (column instanceof IColumn) {
            IColumn col = getIColumn(column);
            if (col.getAlias() != null) {
                return col.getAlias();
            } else {
                return col.getColumnName();
            }

        } else if (column instanceof IFunction) {
            IFunction func = ((IFunction) column);
            if (func.getAlias() != null) {
                return func.getAlias();
            } else {
                return func.getColumnName();
            }
        } else {
            return String.valueOf(column);
        }
    }

    public static String getColumnNameWithTableName(Object column) {
        if (column instanceof IColumn) {
            IColumn col = getIColumn(column);
            String colName = getRealTableName(col.getTableName()) + "." + col.getColumnName();
            return colName;
        } else if (column instanceof IFunction) {
            IFunction func = ((IFunction) column);
            StringBuilder sb = new StringBuilder();
            sb.append(func.getFunctionName());
            sb.append("(");
            boolean first = true;
            if (func.isDistinct()) {
                sb.append("DISTINCT ");
            }
            for (Object arg : func.getArgs()) {
                if (first) {
                    first = false;
                } else {
                    sb.append(",");
                }
                sb.append(getColumnName(arg));
            }
            sb.append(")");
            return sb.toString();
        } else {
            return String.valueOf(column);
        }
    }

    public static DataType getDataType(Object column) {
        if (column instanceof IColumn) {
            return getIColumn(column).getDataType();
        } else if (column instanceof IFunction) {
            return ((IFunction) column).getDataType();
        } else {
            throw new IllegalArgumentException("not support yet");
        }
    }

    public static IColumn getIColumn(Object column) {
        if (column instanceof IFunction) {
            return ASTNodeFactory.getInstance()
                .createColumn()
                .setTableName(((IFunction) column).getTableName())
                .setColumnName(((IFunction) column).getColumnName())
                .setAlias(((IFunction) column).getAlias())
                .setDataType(((IFunction) column).getDataType());
        } else if (!(column instanceof IColumn)) {
            throw new IllegalArgumentException("column :" + column + " is not a icolumn");
        }
        return (IColumn) column;
    }

    public static String getLogicTableName(String indexName) {
        if (indexName == null) {
            return null;
        }
        int index = indexName.indexOf(".");
        if (index != -1) {
            return indexName.substring(0, index);
        } else {
            return indexName;
        }
    }

    public static List<IOrderBy> getOrderBy(IndexMeta meta) {
        if (meta == null) return new ArrayList<IOrderBy>(0);
        List<IOrderBy> _orderBys = new ArrayList<IOrderBy>();
        for (ColumnMeta c : meta.getKeyColumns()) {
            IColumn column = ASTNodeFactory.getInstance().createColumn();
            column.setTableName(c.getTableName())
                .setColumnName(c.getName())
                .setDataType(c.getDataType())
                .setAlias(c.getAlias());
            IOrderBy orderBy = new OrderBy().setColumn(column).setDirection(true);
            _orderBys.add(orderBy);
        }
        return _orderBys;
    }

    public static String getRealTableName(String indexName) {
        if (indexName == null) {
            return null;
        }
        if (indexName.contains(".")) {
            StringBuilder tableName = new StringBuilder();
            String[] tmp = TStringUtil.split(indexName, ".");
            tableName.append(tmp[0]);

            Matcher matcher = pattern.matcher(tmp[1]);
            if (matcher.find()) {
                tableName.append("_").append(matcher.group());
            }
            return tableName.toString();
        } else {
            return indexName;
        }
    }

    /**
     * 将Wrapper实际包含的rowset中的代理去掉
     * 
     * @param rsrs
     * @return
     */
    private static IRowSet IRowSetWrapperToArrayRowSet(RowSetWrapper rsrs) {
        if (rsrs == null) {
            return null;
        }

        IRowSet irs = rsrs.getParentRowSet();
        if (irs instanceof ResultSetRowSet) {
            List<Object> values = rsrs.getValues();
            IRowSet rs = new ArrayRowSet(irs.getParentCursorMeta(), values.toArray());
            rsrs.setParentRowSet(rs);
        } else if (irs instanceof RowSetWrapper) {
            IRowSetWrapperToArrayRowSet((RowSetWrapper) irs);
        }

        return rsrs;
    }

    public static boolean isEqual(Object o1, Object o2) {
        if (o1 == o2) return true;

        return o1 == null ? o2.equals(o1) : o1.equals(o2);
    }

    public static Object getTargetValueByKey(KVPair kv, String name) {
        Object o = null;
        if (kv.getValue() != null) {
            o = kv.getValue().get(name);
        }
        if (o == null && kv.getKey() != null) {
            o = kv.getKey().get(name);
        }
        return o;
    }

    /**
     * 根据column meta取value
     * 
     * @param from_kv
     * @param to_meta
     * @param icol
     * @return
     */
    public static Object getValueByColumnMeta(IRowSet from_kv, ColumnMeta columnMeta) {
        return getValueByTableAndName(from_kv, columnMeta.getTableName(), columnMeta.getName());
    }

    public static Object getValueByIColumn(IRowSet from_kv, ISelectable c) {
        if (from_kv == null) return null;
        Integer index = from_kv.getParentCursorMeta().getIndex(c.getTableName(), c.getColumnName());
        if (index == null) return null;
        Object v = from_kv.getObject(index);
        return v;
    }

    public static Object getValueByTableAndName(IRowSet from_kv, String tableName, String columnName) {
        Integer index = from_kv.getParentCursorMeta().getIndex(tableName, columnName);

        if (index == null) {
            // throw new
            // RuntimeException(tableName+"."+columnName+" is not in "+from_kv);
            return null;
        }
        Object v = from_kv.getObject(index);

        return v;
    }

    public static List<ISelectable> getIColumnsWithISelectable(ColumnMeta[] columns) {
        List<ISelectable> _columns = new ArrayList<ISelectable>(columns.length);
        for (ColumnMeta c : columns) {
            IColumn ic = getIColumnsFromColumnMeta(c);
            _columns.add(ic);

        }
        return _columns;
    }

    public static IColumn getIColumnsFromColumnMeta(ColumnMeta c) {
        IColumn ic = ASTNodeFactory.getInstance().createColumn();
        ic.setColumnName(c.getName());
        ic.setTableName(c.getTableName());

        ic.setDataType(c.getDataType());
        ic.setAlias(c.getAlias());
        if (ic.getDataType() == null) throw new RuntimeException("fuck!");
        return ic;
    }

    public static IColumn getIColumnsFromColumnMeta(ColumnMeta c, String tableAlias) {
        IColumn ic = ASTNodeFactory.getInstance().createColumn();
        ic.setColumnName(c.getName());
        ic.setTableName(tableAlias);

        ic.setDataType(c.getDataType());
        ic.setAlias(c.getAlias());
        if (ic.getDataType() == null) throw new RuntimeException("fuck!");
        return ic;
    }

    public static List<Object> getIColumnsWithISelectable(List<ColumnMeta> columns) {
        List<Object> _columns = new ArrayList<Object>(columns.size());
        for (ColumnMeta c : columns) {
            IColumn ic = ASTNodeFactory.getInstance().createColumn();
            ic.setColumnName(c.getName());
            ic.setTableName(c.getTableName());

            ic.setDataType(c.getDataType());
            ic.setAlias(c.getAlias());
            _columns.add(ic);

            // if (ic.getDataType() == null)
            // throw new RuntimeException("fuck!");
        }
        return _columns;
    }

    public static List<ColumnMeta> getColumnMetas(List<ISelectable> columns) {
        List<ColumnMeta> _columns = new ArrayList<ColumnMeta>(columns.size());
        for (ISelectable c : columns) {
            ColumnMeta ic = new ColumnMeta(c.getTableName(), c.getColumnName(), c.getDataType(), c.getAlias(), true);
            _columns.add(ic);
        }
        return _columns;
    }

    public static List<ColumnMeta> getColumnMetaWithLogicTables(List<ISelectable> columns) {
        List<ColumnMeta> _columns = new ArrayList<ColumnMeta>(columns.size());
        for (ISelectable c : columns) {
            ColumnMeta ic = new ColumnMeta(getLogicTableName(c.getTableName()),
                c.getColumnName(),
                c.getDataType(),
                c.getAlias(),
                true);
            _columns.add(ic);

            // if (ic.getDataType() == null)
            // throw new RuntimeException("fuck!");
        }
        return _columns;
    }

    public static List<ColumnMeta> getColumnMetaWithLogicTablesFromOrderBys(List<IOrderBy> columns) {
        List<ColumnMeta> _columns = new ArrayList<ColumnMeta>(columns.size());
        for (IOrderBy c : columns) {
            ColumnMeta ic = new ColumnMeta(getLogicTableName(c.getTableName()),
                c.getColumnName(),
                c.getDataType(),
                c.getAlias(),
                true);
            _columns.add(ic);
        }
        return _columns;
    }

    public static String getAggColumnName(String columnName, String agg) {
        // 要把函数名前的merge去掉
        // MERGECOUNT变成COUNT

        return (agg + "(" + columnName + ")").replace("MERGE", "");
    }

    public static List<ColumnMeta> convertIColumnsToColumnMeta(List<Object> targetCOlumn) {
        List<ColumnMeta> colMetas = new ArrayList<ColumnMeta>(targetCOlumn.size());
        for (Object icolObj : targetCOlumn) {
            colMetas.add(getColumnMeta(icolObj));
        }
        return colMetas;
    }

    public static List<IColumn> convertColumnMetaToIColumn(List<ColumnMeta> targetCOlumn) {
        List<IColumn> icolumns = new ArrayList<IColumn>(targetCOlumn.size());
        for (ColumnMeta cm : targetCOlumn) {
            IColumn c = ASTNodeFactory.getInstance().createColumn();
            c.setDataType(cm.getDataType());
            c.setColumnName(cm.getName());
            c.setTableName(cm.getTableName());
            c.setAlias(cm.getAlias());
            icolumns.add(c);
        }
        return icolumns;
    }

    public static List<ColumnMeta> convertISelectablesToColumnMeta(List<ISelectable> targetCOlumn) {
        List<ColumnMeta> colMetas = new ArrayList<ColumnMeta>(targetCOlumn.size());
        for (ISelectable icolObj : targetCOlumn) {
            colMetas.add(getColumnMeta(icolObj));
        }
        return colMetas;
    }

    public static List<ColumnMeta> convertISelectablesToColumnMeta(List<ISelectable> columns, String tableAlias,
                                                                   Boolean isSubQuery) {
        List<ColumnMeta> colMetas = new ArrayList<ColumnMeta>(columns.size());
        for (ISelectable c : columns) {
            // if (c instanceof IFunction && !isSubQuery) {
            // colMetas.add(new ColumnMeta(null , c.getAlias() == null ? c
            // .getColumnName() : c.getAlias(), c.getDataType(), c
            // .getAlias()));
            // } else { IFunction 没有table名，columnMeta里要有
            colMetas.add(new ColumnMeta(tableAlias == null ? c.getTableName() : tableAlias,
                c.getAlias() == null ? c.getColumnName() : c.getAlias(),
                c.getDataType(),
                c.getAlias(),
                true));
            // }
        }
        return colMetas;
    }

    /**
     * 根据order by 条件，从left和right KVPair里面拿到一个列所对应的值(从key或者从value里面） 然后进行比较。
     * 相等则继续比较其他。 不相等则根据asc desc决定大小。
     * 
     * @param orderBys
     * @return
     */
    public static Comparator<IRowSet> getComp(final List<IOrderBy> orderBys, final ICursorMeta meta) {
        return new Comparator<IRowSet>() {

            public IRowsValueScaner  leftScaner;
            public IRowsValueScaner  rightScaner;
            public List<ISelectable> cols;
            {
                cols = new ArrayList<ISelectable>();
                for (IOrderBy ord : orderBys) {
                    cols.add(ord.getColumn());
                }
                leftScaner = new RowsValueScanerImp(meta, cols);
            }

            @Override
            public int compare(IRowSet o1, IRowSet o2) {
                if (rightScaner == null) {
                    ICursorMeta rightCursorMeta = o2.getParentCursorMeta();
                    rightScaner = new RowsValueScanerImp(rightCursorMeta, cols);
                }
                Iterator<Object> leftIter = leftScaner.rowValueIterator(o1);
                Iterator<Object> rightIter = rightScaner.rowValueIterator(o2);
                for (IOrderBy orderBy : orderBys) {
                    Comparable c1 = (Comparable) leftIter.next();
                    Comparable c2 = (Comparable) rightIter.next();

                    if (c1 == null && c2 == null) {
                        continue;
                    }
                    int n = comp(c1, c2, orderBy);

                    if (n == 0) continue;

                    return n;

                }
                return 0;
            }
        };

    }

    public static Comparator<IRowSet> getComp(final List<ISelectable> left_columns,
                                              final List<ISelectable> right_columns, final ICursorMeta leftMeta,
                                              final ICursorMeta rightMeta) {
        return new Comparator<IRowSet>() {

            public IRowsValueScaner leftScaner  = new RowsValueScanerImp(leftMeta, left_columns);
            public IRowsValueScaner rightScaner = new RowsValueScanerImp(rightMeta, right_columns);

            /*
             * 沈洵
             * 这个代码与com.taobao.ustore.spi.cursor.common.SortCursor.getComp(List
             * <IOrderBy> orderBys)
             * 唯一的不同，也只有compare的时候需要两个列的list,并且没有asc和desc条件。其他逻辑类似。
             */
            @Override
            public int compare(IRowSet o1, IRowSet o2) {
                Iterator<Object> leftIter = leftScaner.rowValueIterator(o1);
                Iterator<Object> rightIter = rightScaner.rowValueIterator(o2);
                for (int i = 0; i < left_columns.size(); i++) {
                    Comparable c1 = (Comparable) leftIter.next();
                    Comparable c2 = (Comparable) rightIter.next();
                    int n = comp(c1, c2, left_columns.get(i).getDataType(), right_columns.get(i).getDataType());
                    if (n != 0) {
                        return n;
                    }
                }
                return 0;
            }
        };
    }

    public static int comp(Object c1, Object c2, DataType type1, DataType type2) {

        if (type1 == null) {
            type1 = DataTypeUtil.getTypeOfObject(c1);
        }

        // 类型相同，直接比较
        if (type1 == type2) {
            return type1.compare(c1, c2);
        }

        // 类型不同，先进行类型转换
        c2 = type1.convertFrom(c2);
        return type1.compare(c1, c2);
    }

    public static int comp(Comparable c1, Comparable c2, IOrderBy order) {
        DataType type = order.getColumn().getDataType();
        if (type == null) {
            type = DataTypeUtil.getTypeOfObject(c1);
        }

        int n = type.compare(c1, c2);
        if (n == 0) {
            return n;
        }
        boolean isAsc = order.getDirection();
        if (isAsc) {
            return n;
        } else {
            return n < 0 ? 1 : -1;
        }
    }

    public static List<IOrderBy> copyOrderBys(List<IOrderBy> orders) {
        if (orders == null) return null;

        List<IOrderBy> news = new ArrayList(orders.size());
        for (IOrderBy c : orders) {
            news.add(c.copy());
        }

        return news;
    }

    public static CloneableRecord convertToClonableRecord(IRowSet iRowSet) {
        Iterator<ColMetaAndIndex> columnIterator = iRowSet.getParentCursorMeta().indexIterator();
        CloneableRecord cr = new FixedLengthRecord(iRowSet.getParentCursorMeta().getColumns());
        while (columnIterator.hasNext()) {
            ColMetaAndIndex cmAndIndex = columnIterator.next();
            cr.put(cmAndIndex.getName(), iRowSet.getObject(cmAndIndex.getIndex()));
        }
        return cr;
    }

    public static ICursorMeta convertToICursorMeta(IQueryTree query) {
        if (query.getSql() != null) {
            return null;
        }
        ICursorMeta iCursorMeta = null;
        List<ColumnMeta> columns = convertISelectablesToColumnMeta(query.getColumns(),
            query.getAlias(),
            query.isSubQuery());
        iCursorMeta = CursorMetaImp.buildNew(columns, columns.size());
        return iCursorMeta;
    }

    public static ICursorMeta convertToICursorMeta(IndexMeta meta) {
        ICursorMeta iCursorMeta = null;
        List<ColumnMeta> columns = new ArrayList<ColumnMeta>();
        List<ColumnMeta> columnMeta = meta.getKeyColumns();
        addNewColumnMeta(columnMeta, columns);
        columnMeta = meta.getValueColumns();
        addNewColumnMeta(columnMeta, columns);
        iCursorMeta = CursorMetaImp.buildNew(columns, columns.size());
        return iCursorMeta;
    }

    private static void addNewColumnMeta(List<ColumnMeta> columnMeta, List<ColumnMeta> columns) {
        for (ColumnMeta cm : columnMeta) {
            ColumnMeta cmNew = new ColumnMeta(getLogicTableName(cm.getTableName()),
                cm.getName(),
                cm.getDataType(),
                cm.getAlias(),
                cm.isNullable());
            columns.add(cmNew);
        }
    }

    public static Object getObject(final ICursorMeta meta, IRowSet rowSet, String tableName, String columnName) {
        Integer index = meta.getIndex(tableName, columnName);
        if (index == null) {
            throw new RuntimeException("在meta中没找到该列:" + tableName + "." + columnName + " ICursorMeta:" + meta);
        }
        return rowSet.getObject(index);
    }

    public static List<ISelectable> copySelectables(List<ISelectable> cs) {
        if (cs == null) return null;

        List<ISelectable> news = new ArrayList(cs.size());
        for (ISelectable c : cs) {
            news.add(c.copy());
        }

        return news;
    }
}
