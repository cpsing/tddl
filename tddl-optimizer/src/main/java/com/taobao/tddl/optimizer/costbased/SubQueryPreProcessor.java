package com.taobao.tddl.optimizer.costbased;

import java.util.List;

import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.exceptions.OptimizerException;
import com.taobao.tddl.optimizer.exceptions.QueryException;
import com.taobao.tddl.optimizer.utils.FilterUtils;

/**
 * 预先处理子查询
 * 
 * <pre>
 * 1. 尝试将子查询调整为join
 * 
 * 比如 select * from table1 where table1.id = (select id from table2) 
 * 转化为 selct table1.* from table1 join table2 on (table1.id = table2.id)
 * 
 * 
 * 链接：http://dev.mysql.com/doc/refman/5.6/en/subquery-optimization.html
 * </pre>
 * 
 * @author jianghang 2013-12-19 下午6:44:55
 * @since 5.0.0
 */
public class SubQueryPreProcessor {

    public static QueryTreeNode optimize(QueryTreeNode qtn) throws QueryException {
        qtn = findComparisonsSubQueryToJoinNode(qtn);
        return qtn;
    }

    /**
     * 处理下 = in的子查询转化为join
     * http://dev.mysql.com/doc/refman/5.0/en/comparisons-using-subqueries.html
     */
    private static QueryTreeNode findComparisonsSubQueryToJoinNode(QueryTreeNode qtn) {
        for (int i = 0; i < qtn.getChildren().size(); i++) {
            ASTNode ast = qtn.getChildren().get(i);
            if (!(ast instanceof QueryTreeNode)) {
                continue;
            }

            qtn.getChildren().set(i, findComparisonsSubQueryToJoinNode((QueryTreeNode) ast));
        }

        SubQueryAndFilter find = new SubQueryAndFilter();
        find.query = qtn;
        find.filter = null;
        SubQueryAndFilter result = buildSubQuery(find,
            qtn.getWhereFilter(),
            !FilterUtils.isCNFNode(qtn.getWhereFilter()));
        if (result != find) {
            // 如果出现filter，代表where条件中没有组合条件，只有单自查询的条件，直接替换即可
            result.query.query(result.filter);
            qtn.query("");
            return result.query;
        } else {
            return qtn; // 没变化
        }
    }

    private static class SubQueryAndFilter {

        QueryTreeNode query; // 自查询可能会改变query节点为join node
        IFilter       filter; // 子查询带上来的filter
    }

    private static SubQueryAndFilter buildSubQuery(SubQueryAndFilter qtn, IFilter filter, boolean existOr) {
        if (filter instanceof IBooleanFilter) {
            Object column = ((IBooleanFilter) filter).getColumn();
            Object value = ((IBooleanFilter) filter).getValue();
            if (filter.getOperation() == OPERATION.IN) {
                value = ((IBooleanFilter) filter).getValues().get(0);
            }

            boolean columnIsSubQuery = (column != null && column instanceof QueryTreeNode);
            boolean valueIsSubQuery = (value != null && value instanceof QueryTreeNode);
            if (columnIsSubQuery && valueIsSubQuery) {
                throw new OptimizerException("条件左右两边都是子查询,暂时不支持...");
            } else if (columnIsSubQuery || valueIsSubQuery) {
                if (existOr) {
                    throw new OptimizerException("出现子查询条件时,不支持和另一个条件的OR关系");
                }

                QueryTreeNode query = (QueryTreeNode) (columnIsSubQuery ? column : value);
                Object c = columnIsSubQuery ? value : column;
                if (!(c instanceof ISelectable)) {
                    throw new OptimizerException("不支持常量和子查询组合的条件");
                }

                if (!query.getOrderBys().isEmpty() || query.isExistAggregate()) {
                    throw new OptimizerException("子查询存在聚合查询条件太复杂，无法转化为semi-join");
                }

                ISelectable leftColumn = (ISelectable) c;
                // 出现子查询
                if (filter.getOperation() == OPERATION.EQ || filter.getOperation() == OPERATION.IN) {
                    if (query.getColumnsSelected().size() > 1) {
                        throw new OptimizerException("条件子查询只能返回一个字段");
                    }

                    SubQueryAndFilter result = new SubQueryAndFilter();
                    ISelectable rightColumn = query.getColumnsSelected().get(0);

                    List<ISelectable> newSelects = qtn.query.getColumnsSelected();
                    result.query = qtn.query.join(query).addJoinKeys(leftColumn, rightColumn);
                    result.query.select(newSelects);

                    if (filter.getOperation() == OPERATION.IN && filter.isNot()) {
                        // not in的语法
                        // http://dev.mysql.com/doc/refman/5.6/en/rewriting-subqueries.html
                        IBooleanFilter f = ASTNodeFactory.getInstance().createBooleanFilter();
                        f.setOperation(OPERATION.IS_NULL);
                        f.setColumn(rightColumn);
                        result.filter = and(f, query.getWhereFilter());
                        query.query("");// 清空底下的查询条件
                    } else {
                        IBooleanFilter f = ASTNodeFactory.getInstance().createBooleanFilter();
                        f.setOperation(OPERATION.CONSTANT);
                        f.setColumn("1");
                        result.filter = and(f, query.getWhereFilter());
                        query.query("");// 清空底下的查询条件
                    }
                    return result;
                } else {
                    throw new OptimizerException("条件子查询,暂时不支持操作符:" + filter.getOperation().getOPERATIONString());
                }
            } else {
                qtn.filter = filter;
                return qtn; // 没有子查询
            }
        } else if (filter instanceof ILogicalFilter) {
            ILogicalFilter logical = (ILogicalFilter) filter;
            for (int i = 0; i < logical.getSubFilter().size(); i++) {
                SubQueryAndFilter result = buildSubQuery(qtn, logical.getSubFilter().get(i), existOr);
                if (result != qtn) {
                    logical.getSubFilter().set(i, result.filter);
                    qtn = result;
                }
            }

            qtn.filter = logical;
        }

        return qtn;
    }

    private static IFilter and(IFilter filter, IFilter subQueryFilter) {
        if (subQueryFilter == null) {
            return filter;
        }

        ILogicalFilter and = ASTNodeFactory.getInstance().createLogicalFilter().setOperation(OPERATION.AND);
        and.addSubFilter(filter);
        and.addSubFilter(subQueryFilter);
        return and;
    }

}
