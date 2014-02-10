package com.taobao.tddl.optimizer.costbased;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.query.JoinNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.exceptions.EmptyResultFilterException;
import com.taobao.tddl.optimizer.exceptions.QueryException;
import com.taobao.tddl.optimizer.utils.FilterUtils;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

/**
 * 预先处理filter条件
 * 
 * <pre>
 * 1. 判断永真/永假式,短化路径
 * 如： false or a = 1，优化为 a = 1
 * 如： true or a = 1，优化为true
 * 如： false or false，优化为{@linkplain EmptyResultFilterException}异常
 * 
 * 2. 调整下1 = id的列信息，优化为id = 1
 * 如：1 < a，调整为 a > 1 
 * 如：1 <= a，调整为 a >= 1
 * 如：1 > a，调整为 a < 1
 * 如：1 >= a，调整为 a <= 1
 * 其他情况，仅仅是交换下位置
 * 
 * 3. 根据column列，调整下value的类型
 * 如：a = 1，如果a是LONG型，会将文本1转化为Long. (sql解析后都会是纯文本信息)
 * </pre>
 */
public class FilterPreProcessor {

    /**
     * 处理逻辑见类描述 {@linkplain FilterPreProcessor}
     */
    public static QueryTreeNode optimize(QueryTreeNode qtn, boolean typeConvert) throws QueryException {
        qtn = preProcess(qtn, typeConvert);
        return qtn;
    }

    private static QueryTreeNode preProcess(QueryTreeNode qtn, boolean typeConvert) throws QueryException {
        qtn.setOtherJoinOnFilter(processFilter(qtn.getOtherJoinOnFilter(), typeConvert));
        qtn.having(processFilter(qtn.getHavingFilter(), typeConvert));
        qtn.query(processFilter(qtn.getWhereFilter(), typeConvert));
        qtn.setKeyFilter(processFilter(qtn.getKeyFilter(), typeConvert));
        qtn.setResultFilter(processFilter(qtn.getResultFilter(), typeConvert));
        if (qtn instanceof TableNode) {
            ((TableNode) qtn).setIndexQueryValueFilter(processFilter(((TableNode) qtn).getIndexQueryValueFilter(),
                typeConvert));
        }

        if (qtn instanceof JoinNode) {
            for (int i = 0; i < ((JoinNode) qtn).getJoinFilter().size(); i++) {
                processFilter(((JoinNode) qtn).getJoinFilter().get(i), typeConvert);
            }
        }

        for (ASTNode child : qtn.getChildren()) {
            preProcess((QueryTreeNode) child, typeConvert);
        }

        return qtn;
    }

    private static IFilter processFilter(IFilter root, boolean typeConvert) {
        if (root == null) {
            return null;
        }

        root = shortestFilter(root); // 短路一下
        root = processOneFilter(root, typeConvert); // 做一下转换处理
        root = FilterUtils.merge(root);// 合并一下flter
        return root;
    }

    private static IFilter processOneFilter(IFilter root, boolean typeConvert) {
        if (root == null) {
            return null;
        }

        if (root instanceof IBooleanFilter) {
            return processBoolFilter(root, typeConvert);
        } else if (root instanceof ILogicalFilter) {
            ILogicalFilter lf = (ILogicalFilter) root;
            List<IFilter> children = new LinkedList<IFilter>();
            for (IFilter child : lf.getSubFilter()) {
                IFilter childProcessed = processOneFilter(child, typeConvert);
                if (childProcessed != null) {
                    children.add(childProcessed);
                }
            }

            if (children.isEmpty()) {
                return null;
            }

            if (children.size() == 1) {
                return children.get(0);
            }

            lf.setSubFilter(children);
            return lf;
        }

        return root;
    }

    /**
     * 将0=1/1=1/true的恒等式进行优化
     */
    private static IFilter shortestFilter(IFilter root) throws EmptyResultFilterException {
        IFilter filter = FilterUtils.toDNFAndFlat(root);
        List<List<IFilter>> DNFfilter = FilterUtils.toDNFNodesArray(filter);

        List<List<IFilter>> newDNFfilter = new ArrayList<List<IFilter>>();
        for (List<IFilter> andDNFfilter : DNFfilter) {
            boolean isShortest = false;
            List<IFilter> newAndDNFfilter = new ArrayList<IFilter>();
            for (IFilter one : andDNFfilter) {
                if (one.getOperation() == OPERATION.CONSTANT) {
                    boolean flag = false;
                    if (((IBooleanFilter) one).getColumn() instanceof ISelectable) {// 可能是个not函数
                        newAndDNFfilter.add(one);// 不能丢弃
                    } else {
                        String value = ((IBooleanFilter) one).getColumn().toString();
                        if (StringUtils.isNumeric(value)) {
                            flag = BooleanUtils.toBoolean(Integer.valueOf(value));
                        } else {
                            flag = BooleanUtils.toBoolean(((IBooleanFilter) one).getColumn().toString());
                        }
                        if (!flag) {
                            isShortest = true;
                            break;
                        }
                    }
                } else {
                    newAndDNFfilter.add(one);
                }
            }

            if (!isShortest) {
                if (newAndDNFfilter.isEmpty()) {
                    // 代表出现为true or xxx，直接返回true
                    IBooleanFilter f = ASTNodeFactory.getInstance().createBooleanFilter();
                    f.setOperation(OPERATION.CONSTANT);
                    f.setColumn("1");
                    f.setColumnName(ObjectUtils.toString("1"));
                    return f;
                } else {// 针对非false的情况
                    newDNFfilter.add(newAndDNFfilter);
                }
            }
        }

        if (newDNFfilter.isEmpty()) {
            throw new EmptyResultFilterException("空结果");
        }

        return FilterUtils.DNFToOrLogicTree(newDNFfilter);
    }

    private static IFilter processBoolFilter(IFilter root, boolean typeConvert) {
        root = exchage(root);

        if (typeConvert) {
            root = typeConvert(root);
        }
        return root;
    }

    /**
     * 如果是1 = id的情况，转化为id = 1
     */
    private static IFilter exchage(IFilter root) {
        IBooleanFilter bf = (IBooleanFilter) root;
        if (!FilterUtils.isConstValue(bf.getValue()) && FilterUtils.isConstValue(bf.getColumn())) {
            Object val = bf.getColumn();
            bf.setColumn(bf.getValue());
            bf.setValue(val);
            OPERATION newOp = bf.getOperation();
            switch (bf.getOperation()) {
                case GT:
                    newOp = OPERATION.LT;
                    break;
                case LT:
                    newOp = OPERATION.GT;
                    break;
                case GT_EQ:
                    newOp = OPERATION.LT_EQ;
                    break;
                case LT_EQ:
                    newOp = OPERATION.GT_EQ;
                    break;
                default:
                    break;
            }
            bf.setOperation(newOp);
        }
        return bf;
    }

    private static IFilter typeConvert(IFilter root) {
        IBooleanFilter bf = (IBooleanFilter) root;
        // 如果是id in (xx)
        if (bf.getValues() != null) {
            if (bf.getColumn() instanceof IColumn) {
                for (int i = 0; i < bf.getValues().size(); i++) {
                    bf.getValues().set(i,
                        OptimizerUtils.convertType(bf.getValues().get(i), ((IColumn) bf.getColumn()).getDataType()));
                }
            }
        } else {
            // 如果是 1 = id情况
            if (FilterUtils.isConstValue(bf.getColumn()) && !FilterUtils.isConstValue(bf.getValue())) {
                DataType type = null;
                if (bf.getValue() instanceof IColumn) {
                    type = ((IColumn) bf.getValue()).getDataType();
                }

                // if (bf.getValue() instanceof IFunction) {
                // type = ((IFunction) bf.getValue()).getDataType();
                // }

                bf.setColumn(OptimizerUtils.convertType(bf.getColumn(), type));
            }

            // 如果是 id = 1情况
            if (FilterUtils.isConstValue(bf.getValue()) && !FilterUtils.isConstValue(bf.getColumn())) {
                DataType type = null;
                if (bf.getColumn() instanceof IColumn) {
                    type = ((IColumn) bf.getColumn()).getDataType();
                }

                // if (bf.getColumn() instanceof IFunction) {
                // type = ((IFunction) bf.getColumn()).getDataType();
                // }

                bf.setValue(OptimizerUtils.convertType(bf.getValue(), type));
            }
        }
        return bf;
    }
}
