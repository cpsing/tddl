package com.taobao.tddl.optimizer.utils;

import java.util.List;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * {@linkplain IFilter}的toString()方法
 * 
 * @author mengshi.sunmengshi
 * @author <a href="jianghang.loujh@taobao.com">jianghang</a>
 */
public class OptimizerToString {

    public static String printFilterString(IFilter filter) {
        return printFilterString(filter, 0, true);
    }

    public static String printFilterString(IFilter filter, int inden) {
        return printFilterString(filter, inden, true);
    }

    public static String getTab(int count) {
        StringBuffer tab = new StringBuffer();
        for (int i = 0; i < count; i++)
            tab.append("    ");
        return tab.toString();
    }

    /**
     * 根据给定的inden进行拼接filed : value
     * 
     * @param sb
     * @param field
     * @param value
     * @param inden
     */
    public static void appendField(StringBuilder sb, String field, Object value, String inden) {
        if (value == null || value.toString().equals("") || value.toString().equals("[]")
            || value.toString().equals("SEQUENTIAL") || value.toString().equals("SHARED_LOCK")) {
            return;
        }

        appendln(sb, inden + field + ":" + value);
    }

    /**
     * 拼接换行符
     */
    public static void appendln(StringBuilder sb, String v) {
        sb.append(v).append("\n");
    }

    public static String printFilterString(IFilter filter, int inden, boolean needTable) {
        if (filter == null) {
            return null;
        }

        if (filter instanceof IBooleanFilter) {
            StringBuilder builder = new StringBuilder();
            if (filter.isNot()) {
                builder.append("NOT (");
            }

            IBooleanFilter bf = (IBooleanFilter) filter;
            if (bf.getColumn() != null && (bf.getValue() != null || bf.getValues() != null)) {
                if (bf.getOperation().equals(OPERATION.IN)) {
                    builder.append(getColumnName(bf, inden, needTable))
                        .append(" ")
                        .append(bf.getOperation().getOPERATIONString())
                        .append(" ")
                        .append(getValueName(bf, inden, needTable, true));
                } else {
                    builder.append(getColumnName(bf, inden, needTable))
                        .append(" ")
                        .append(bf.getOperation().getOPERATIONString())
                        .append(" ")
                        .append(getValueName(bf, inden, needTable, false));
                }
            } else if (bf.getOperation().equals(OPERATION.IS_NULL) || bf.getOperation().equals(OPERATION.IS_NOT_NULL)) {
                builder.append(getColumnName(bf, inden, needTable))
                    .append(" ")
                    .append(bf.getOperation().getOPERATIONString());
            } else {
                builder.append(getColumnName(bf, inden, needTable));
            }

            if (filter.isNot()) {
                builder.append(")");
            }

            return builder.toString();
        } else if (filter instanceof ILogicalFilter) {
            ILogicalFilter lf = (ILogicalFilter) filter;
            StringBuilder builder = new StringBuilder("(");

            if (filter.isNot()) {
                builder.append("NOT (");
            }

            builder.append(printFilterString(lf.getSubFilter().get(0)));
            for (int i = 1; i < lf.getSubFilter().size(); i++) {
                builder.append(" ")
                    .append(lf.getOperation().getOPERATIONString())
                    .append(" ")
                    .append(printFilterString(lf.getSubFilter().get(i)));
            }

            if (filter.isNot()) {
                builder.append(")");
            }

            builder.append(")");
            return builder.toString();
        } else {
            throw new NotSupportException();
        }
    }

    private static String getColumnName(IBooleanFilter bf, int inden, boolean needTable) {
        if (bf == null) {
            return null;
        }

        if (needTable) {
            return bf.getColumn().toString();
        } else {
            List args = bf.getArgs();
            if (!args.isEmpty()) {
                Object obj = args.get(0);
                if (obj instanceof ISelectable) {
                    return ((ISelectable) obj).getColumnName();
                } else if (obj instanceof Comparable) {
                    return obj.toString();
                }
            }

            return null;
        }
    }

    private static String getValueName(IBooleanFilter bf, int inden, boolean needTable, boolean isIn) {
        if (bf == null) {
            return null;
        }
        if (needTable) {
            if (isIn) {
                return bf.getValues().toString();
            } else {
                if (bf.getValue() instanceof QueryTreeNode) {
                    return "\n" + ((QueryTreeNode) bf.getValue()).toString(inden);
                } else {
                    return bf.getValue().toString();
                }
            }
        } else {
            List args = bf.getArgs();
            if (args.size() == 2) {
                Object obj = args.get(1);
                if (obj instanceof ISelectable) {
                    return ((ISelectable) obj).getColumnName();
                } else if (obj instanceof Comparable) {
                    return obj.toString();
                }
            }

            return null;
        }
    }

}
