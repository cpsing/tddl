package com.taobao.tddl.optimizer.costbased.after;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.QueryPlanOptimizer;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IFunction.FunctionType;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;

/**
 * 如果一个aggregate没有merge节点，就加一个merge节点进去。 会添加一个大的merge节点上去。
 * 
 * @author Whisper
 */
public class CreateMergeForAggregateOptimizer implements QueryPlanOptimizer {

    @Override
    public IDataNodeExecutor optimize(IDataNodeExecutor dne, Map<Integer, ParameterContext> parameterSettings,
                                      Map<String, Comparable> extraCmd) {
        if (dne instanceof IQueryTree) {
            if (isContainsAggregateFunctionButHasNoMerge((IQueryTree) dne)) {
                IMerge merge = ASTNodeFactory.getInstance().createMerge();
                List<ISelectable> mergeSelected = new ArrayList(((IQueryTree) dne).getColumns().size());
                for (Object sel : ((IQueryTree) dne).getColumns()) {
                    ISelectable s = (ISelectable) sel;
                    ISelectable sc = s.copy();
                    if (s.getAlias() != null) {
                        sc.setColumnName(s.getAlias());
                    }
                    mergeSelected.add(sc);
                }
                merge.setColumns(mergeSelected);
                merge.setConsistent(dne.getConsistent());
                merge.setGroupBys(((IQueryTree) dne).getGroupBys());
                merge.setOrderBys(((IQueryTree) dne).getOrderBys());
                merge.executeOn(dne.getDataNode());
                List<IDataNodeExecutor> subs = new ArrayList(1);
                subs.add(dne);
                merge.setSubNode(subs);
                return merge;
            }
        }
        return dne;
    }

    private boolean isContainsAggregateFunctionButHasNoMerge(IQueryTree qc) {
        if (qc instanceof IMerge) {
            return false;
        }

        for (Object c : qc.getColumns()) {
            if (c instanceof IFunction) {
                if (((IFunction) c).getFunctionType().equals(FunctionType.Aggregate)) {
                    return true;
                }

                if (this.isArgsContainsAggregateFunction((IFunction) c)) {
                    return true;
                }
            }
        }

        if (qc instanceof IQuery) {
            if (((IQuery) qc).getSubQuery() == null) {
                return false;
            } else {
                return isContainsAggregateFunctionButHasNoMerge(((IQuery) qc).getSubQuery());
            }
        }

        if (qc instanceof IJoin) {
            boolean res = isContainsAggregateFunctionButHasNoMerge(((IJoin) qc).getLeftNode());
            res = res || isContainsAggregateFunctionButHasNoMerge(((IJoin) qc).getRightNode());
            return res;
        }

        throw new IllegalAccessError("这不科学！");
    }

    private boolean isArgsContainsAggregateFunction(IFunction f) {
        for (Object arg : f.getMapArgs()) {
            if (arg instanceof IFunction) {
                if (((IFunction) arg).getFunctionType().equals(FunctionType.Aggregate)) {
                    return true;
                }

                if (isArgsContainsAggregateFunction((IFunction) arg)) {
                    return true;
                }
            }
        }

        return false;
    }
}
