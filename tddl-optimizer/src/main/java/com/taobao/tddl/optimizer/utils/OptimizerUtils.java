package com.taobao.tddl.optimizer.utils;

import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.time.DateFormatUtils;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;
import com.taobao.tddl.optimizer.core.expression.bean.BindVal;
import com.taobao.tddl.optimizer.core.expression.bean.NullValue;
import com.taobao.tddl.rule.exceptions.TddlRuleException;

/**
 * @since 5.1.0
 */
public class OptimizerUtils {

    private static final String[] DATE_FORMATS = new String[] { "yyyy-MM-dd", "HH:mm:ss", "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd hh:mm:ss.S", "EEE MMM dd HH:mm:ss zzz yyyy", DateFormatUtils.ISO_DATETIME_FORMAT.getPattern(),
            DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern(),
            DateFormatUtils.SMTP_DATETIME_FORMAT.getPattern(), };

    public static Comparable convertType(Comparable value, DATA_TYPE type) {
        if (value == null) {
            return null;
        }

        if (type == null || value instanceof BindVal || value instanceof IFunction || value instanceof NullValue) {
            return value;
        }

        String strValue = value.toString();
        try {
            if (type.equals(DATA_TYPE.LONG_VAL)) {
                value = Long.valueOf(strValue);
            } else if (type.equals(DATA_TYPE.INT_VAL)) {
                value = Integer.valueOf(strValue);
            } else if (type.equals(DATA_TYPE.BOOLEAN_VAL)) {
                value = Boolean.valueOf(strValue);
            } else if (type.equals(DATA_TYPE.BYTES_VAL)) {
                value = Byte.valueOf(strValue);
            } else if (type.equals(DATA_TYPE.CHAR_VAL)) {
                value = strValue;
            } else if (type.equals(DATA_TYPE.DATE_VAL) || type.equals(DATA_TYPE.TIMESTAMP)) {
                if (value != null && value instanceof Long) {
                    value = new Date((Long) value);
                }

                if (!(value instanceof Date)) {
                    value = null;
                    try {
                        value = parseDate(strValue.trim(), DATE_FORMATS, Locale.ENGLISH);
                    } catch (Exception err) {
                        try {
                            value = parseDate(strValue.trim(), DATE_FORMATS, Locale.getDefault());
                        } catch (Exception e) {
                            throw new TddlRuleException("unSupport date parse :" + strValue.trim());
                        }
                    }
                }
            } else if (type.equals(DATA_TYPE.DOUBLE_VAL)) {
                value = Double.valueOf(strValue);
            } else if (type.equals(DATA_TYPE.FLOAT_VAL)) {
                value = Float.valueOf(strValue);
            } else if (type.equals(DATA_TYPE.SHORT_VAL)) {
                value = Short.valueOf(strValue);
            } else if (type.equals(DATA_TYPE.STRING_VAL)) {
                value = strValue;
            } else {
                throw new UnsupportedOperationException("Not supported yet : " + type);
            }
        } catch (Exception ex) {
            // 若转换失败，则交给filter的function自己处理
            return value;
        }

        return value;
    }

    private static Date parseDate(String str, String[] parsePatterns, Locale locale) throws ParseException {
        if ((str == null) || (parsePatterns == null)) {
            throw new IllegalArgumentException("Date and Patterns must not be null");
        }

        SimpleDateFormat parser = null;
        ParsePosition pos = new ParsePosition(0);

        for (int i = 0; i < parsePatterns.length; i++) {
            if (i == 0) {
                parser = new SimpleDateFormat(parsePatterns[0], locale);
            } else {
                parser.applyPattern(parsePatterns[i]);
            }
            pos.setIndex(0);
            Date date = parser.parse(str, pos);
            if ((date != null) && (pos.getIndex() == str.length())) {
                return date;
            }
        }

        throw new ParseException("Unable to parse the date: " + str, -1);
    }

    public static ColumnMeta[] uniq(ColumnMeta[] s) {
        if (s == null) return null;
        Set<ColumnMeta> t = new HashSet<ColumnMeta>();
        List<ColumnMeta> uniqList = new ArrayList<ColumnMeta>(s.length);

        for (ColumnMeta cm : s) {
            if (!t.contains(cm)) {
                uniqList.add(cm);
                t.add(cm);
            }
        }
        ColumnMeta uniqArray[] = new ColumnMeta[uniqList.size()];
        for (int i = 0; i < uniqArray.length; i++) {
            uniqArray[i] = uniqList.get(i);
        }
        return uniqArray;
    }

    public static IFilter copyFilter(IFilter f) {
        return (IFilter) (f == null ? null : f.copy());
    }

    public static List<IOrderBy> copyOrderBys(List<IOrderBy> orders) {
        if (orders == null) {
            return null;
        }

        List<IOrderBy> news = new ArrayList(orders.size());
        for (IOrderBy c : orders) {
            news.add(c.deepCopy());
        }

        return news;
    }

    public static Set<ISelectable> copySelectables(Set<ISelectable> cs) {
        if (cs == null) {
            return null;
        }
        Set<ISelectable> news = new HashSet(cs.size());
        for (ISelectable c : cs) {
            news.add(c.copy());
        }

        return news;
    }

    public static List<ISelectable> copySelectables(List<ISelectable> cs) {
        if (cs == null) {
            return null;
        }

        List<ISelectable> news = new ArrayList(cs.size());
        for (ISelectable c : cs) {
            news.add(c.copy());
        }

        return news;
    }

    public static List<IBooleanFilter> deepCopyFilterList(List<IBooleanFilter> filters) {
        if (filters == null) {
            return null;
        }

        List<IBooleanFilter> newFilters = new ArrayList<IBooleanFilter>(filters.size());
        for (IBooleanFilter f : filters) {
            newFilters.add((IBooleanFilter) f.copy());
        }
        return newFilters;
    }

    public static List<IOrderBy> deepCopyOrderByList(ArrayList<IOrderBy> orders) {
        if (orders == null) {
            return null;
        }

        List<IOrderBy> newOrders = new ArrayList<IOrderBy>(orders.size());

        for (IOrderBy o : orders) {
            newOrders.add(o.deepCopy());
        }

        return newOrders;

    }

    public static List<ISelectable> deepCopySelectableList(List<ISelectable> selectableList) {
        if (selectableList == null) {
            return null;
        }

        List<ISelectable> newSelectable = new ArrayList<ISelectable>(selectableList.size());
        for (ISelectable s : selectableList) {
            newSelectable.add(s.copy());
        }

        return newSelectable;
    }

    /**
     * 根据索引信息，构建orderby条件
     */
    public static List<IOrderBy> getOrderBy(IndexMeta meta) {
        if (meta == null) {
            return new ArrayList<IOrderBy>(0);
        }

        List<IOrderBy> _orderBys = new ArrayList<IOrderBy>();
        for (ColumnMeta c : meta.getKeyColumns()) {
            IColumn column = ASTNodeFactory.getInstance().createColumn();
            column.setTableName(c.getTableName())
                .setColumnName(c.getName())
                .setDataType(c.getDataType())
                .setAlias(c.getAlias());
            IOrderBy orderBy = ASTNodeFactory.getInstance().createOrderBy().setColumn(column).setDirection(true);
            _orderBys.add(orderBy);
        }
        return _orderBys;
    }

    /**
     * 根据column string构造{@linkplain ISelectable}对象
     * 
     * @param columnStr
     * @return
     */
    public static ISelectable createColumnFromString(String columnStr) {
        if (columnStr == null) {
            return null;
        }

        if (TStringUtil.containsIgnoreCase(columnStr, "AS")) {
            String tmp[] = TStringUtil.split(columnStr, "AS");
            if (tmp.length != 2) {
                throw new RuntimeException("createColumnFromString:" + columnStr);
            }

            ISelectable c = createColumnFromString(tmp[0].trim());
            c.setAlias(tmp[1].trim());
            return c;
        }

        IColumn c = ASTNodeFactory.getInstance().createColumn();
        if (columnStr.contains(".")) {
            String tmp[] = columnStr.split("\\.");
            c.setColumnName(tmp[1]).setTableName(tmp[0]);
        } else {
            c.setColumnName(columnStr);
        }
        return c;
    }

    public static IColumn columnMetaToIColumn(ColumnMeta m, String tableName) {
        IColumn c = ASTNodeFactory.getInstance().createColumn();
        c.setDataType(m.getDataType());
        c.setColumnName(m.getName());
        c.setTableName(tableName);
        return c;
    }

    public static IColumn getColumn(Object column) {
        return getIColumn(column);
    }

    public static IColumn getIColumn(Object column) {
        if (column instanceof IFunction) {
            return (IColumn) ASTNodeFactory.getInstance()
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

    /**
     * 将columnMeta转化为column列
     */
    public static List<ISelectable> columnMetaListToIColumnList(Collection<ColumnMeta> ms, String tableName) {
        List<ISelectable> cs = new ArrayList(ms.size());
        for (ColumnMeta m : ms) {
            cs.add(columnMetaToIColumn(m, tableName));
        }

        return cs;
    }

    // --------------------------- assignment --------------------------

    public static IFilter assignment(IFilter f, Map<Integer, ParameterContext> parameterSettings) {
        if (f == null) {
            return null;
        }

        return (IFilter) f.assignment(parameterSettings);
    }

    public static ISelectable assignment(ISelectable c, Map<Integer, ParameterContext> parameterSettings) {
        if (c == null) {
            return c;
        }

        return c.assignment(parameterSettings);
    }

    public static List<ISelectable> assignment(List<ISelectable> cs, Map<Integer, ParameterContext> parameterSettings) {
        if (cs == null) {
            return cs;
        }
        for (ISelectable s : cs) {
            assignment(s, parameterSettings);
        }

        return cs;
    }

}
