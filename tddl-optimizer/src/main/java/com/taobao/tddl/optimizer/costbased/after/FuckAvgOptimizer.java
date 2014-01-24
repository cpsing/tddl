package com.taobao.tddl.optimizer.costbased.after;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IFunction.FunctionType;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.expression.bean.Function;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;

/**
 * avg变成count + sum 要改变columns结构
 * 
 * @author Whisper
 */
public class FuckAvgOptimizer implements QueryPlanOptimizer {

    public FuckAvgOptimizer(){
    }

    /**
     * 把query中的avg换成count，sum
     */
    @Override
    public IDataNodeExecutor optimize(IDataNodeExecutor dne, Map<Integer, ParameterContext> parameterSettings,
                                      Map<String, Object> extraCmd) {
        if (dne instanceof IMerge && ((IMerge) dne).getSubNode().size() > 1) {
            boolean hasAvg = false;
            for (IDataNodeExecutor sub : ((IMerge) dne).getSubNode()) {
                hasAvg = hasAvg(sub);
            }

            if (hasAvg) {
                for (IDataNodeExecutor sub : ((IMerge) dne).getSubNode()) {
                    expendAvgFunction(sub);
                }
            }

            for (IDataNodeExecutor sub : ((IMerge) dne).getSubNode()) {
                this.optimize(sub, parameterSettings, extraCmd);
            }
        } else if (dne instanceof IJoin) {
            IJoin join = (IJoin) dne;
            // 如果存在join函数，并且左右子节点的dataNode不同(即是跨机操作)，需要自己处理avg函数
            if (hasAvg(dne) && !join.getLeftNode().getDataNode().equals(join.getRightNode().getDataNode())) {
                expendAvgFunction(dne); // 展开join自己
            }

            // 递归处理子节点
            this.optimize(join, parameterSettings, extraCmd);
            this.optimize(join, parameterSettings, extraCmd);
        } else if (dne instanceof IQuery) {
            IQuery query = (IQuery) dne;
            // 如果是子查询暂时不考虑下推avg
            // 子查询下推avg的条件：当前query节点无where条件/having条件
            if (hasAvg(dne) && query.isSubQuery()) {
                expendAvgFunction(dne); // 展开query自己
            }

            if (query.isSubQuery()) {
                this.optimize(query.getSubQuery(), parameterSettings, extraCmd);// 递归处理子节点
            }
        }

        return dne;
    }

    /**
     * 判断是否有avg函数
     */
    private boolean hasAvg(IDataNodeExecutor sub) {
        if (sub instanceof IQuery || sub instanceof IJoin) {
            for (Object s : ((IQueryTree) sub).getColumns()) {
                if (s instanceof IFunction) {
                    if (((IFunction) s).getFunctionName().startsWith("AVG")) {
                        return true;
                    }
                }
            }

        }
        return false;
    }

    /**
     * 将Avg函数展开为sum/count
     */
    private void expendAvgFunction(IDataNodeExecutor sub) {
        if (sub instanceof IQuery || sub instanceof IJoin) {
            List<ISelectable> add = new ArrayList();
            List<ISelectable> remove = new ArrayList();
            for (Object sel : ((IQueryTree) sub).getColumns()) {
                ISelectable s = (ISelectable) sel;
                if (s instanceof IFunction) {
                    if (FunctionType.Scalar.equals(((IFunction) s).getFunctionType())) {
                        remove.add(s);
                    } else if (s.getColumnName().startsWith("AVG(")) {
                        Function count = (Function) s.copy();
                        count.setExtraFunction(null);
                        count.setFunctionName("COUNT");
                        count.setColumnName(s.getColumnName().replace("AVG(", "COUNT("));

                        Function sum = (Function) s.copy();
                        sum.setExtraFunction(null);
                        sum.setFunctionName("SUM");
                        sum.setColumnName(s.getColumnName().replace("AVG(", "SUM("));
                        add.add(count);
                        add.add(sum);

                        remove.add(s);
                    }
                }
            }

            if (!remove.isEmpty()) {
                ((IQueryTree) sub).getColumns().removeAll(remove);
                ((IQueryTree) sub).getColumns().addAll(add);
            }
        }
    }
}
