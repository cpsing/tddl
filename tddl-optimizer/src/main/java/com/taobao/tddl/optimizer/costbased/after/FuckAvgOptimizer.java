package com.taobao.tddl.optimizer.costbased.after;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IFunction.FunctionType;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
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
            for (IDataNodeExecutor sub : ((IMerge) dne).getSubNode()) {
                expendAvgFunction(sub);
            }

            for (IDataNodeExecutor sub : ((IMerge) dne).getSubNode()) {
                this.optimize(sub, parameterSettings, extraCmd);
            }
        } else if (dne instanceof IJoin) {
            IJoin join = (IJoin) dne;
            // join函数，采取map模式，不需要处理avg展开
            // 递归处理子节点
            this.optimize(join.getLeftNode(), parameterSettings, extraCmd);
            this.optimize(join.getRightNode(), parameterSettings, extraCmd);
        } else if (dne instanceof IQuery) {
            IQuery query = (IQuery) dne;
            // 如果是子查询,采取map模式，不需要处理avg展开
            if (query.isSubQuery()) {
                this.optimize(query.getSubQuery(), parameterSettings, extraCmd);// 递归处理子节点
            }
        }

        return dne;
    }

    private boolean hasArgsAvgFunction(IFunction func) {
        for (Object args : func.getArgs()) {
            if (args instanceof IFunction && ((IFunction) args).getColumnName().startsWith("AVG(")) {
                return true;
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
                    if (s.getColumnName().startsWith("AVG(")) {
                        IFunction sum = (IFunction) s.copy();
                        sum.setExtraFunction(null);
                        sum.setFunctionName("SUM");
                        sum.setColumnName(s.getColumnName().replace("AVG(", "SUM("));
                        if (sum.getAlias() != null) {
                            sum.setAlias(sum.getAlias() + "1");// 加个后缀1
                        }

                        IFunction count = (IFunction) s.copy();
                        count.setExtraFunction(null);
                        count.setFunctionName("COUNT");
                        count.setColumnName(s.getColumnName().replace("AVG(", "COUNT("));
                        if (count.getAlias() != null) {
                            count.setAlias(count.getAlias() + "2");// 加个后缀2
                        }

                        add.add(count);
                        add.add(sum);

                        remove.add(s);
                    } else {
                        // 删除底下AVG的相关函数，比如 1 + AVG(ID)
                        // 目前这个只能上层来进行计算
                        // 可能的风险：还未支持的Function计算
                        if (FunctionType.Scalar.equals(((IFunction) s).getFunctionType())
                            && hasArgsAvgFunction((IFunction) s)) {
                            remove.add(s);
                        }
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
