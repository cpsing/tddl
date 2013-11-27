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
import com.taobao.tddl.optimizer.costbased.before.QueryPlanOptimizer;

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
                                      Map<String, Comparable> extraCmd) {
        if (dne instanceof IMerge && ((IMerge) dne).getSubNode().size() > 1) {
            boolean hasAvg = false;
            for (IDataNodeExecutor sub : ((IMerge) dne).getSubNode()) {
                if (sub instanceof IQuery || sub instanceof IJoin) {
                    for (Object s : ((IQueryTree) sub).getColumns()) {
                        if (s instanceof IFunction) {
                            if (((IFunction) s).getFunctionName().startsWith("AVG")) {
                                hasAvg = true;
                                break;
                            }
                        }
                    }

                    break;
                }
            }

            if (hasAvg) {
                for (IDataNodeExecutor sub : ((IMerge) dne).getSubNode()) {
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
        }
        if (dne instanceof IJoin) {
            this.optimize(((IJoin) dne).getLeftNode(), parameterSettings, extraCmd);
            this.optimize(((IJoin) dne).getRightNode(), parameterSettings, extraCmd);
        }

        if (dne instanceof IMerge) {
            for (IDataNodeExecutor sub : ((IMerge) dne).getSubNode()) {
                this.optimize(sub, parameterSettings, extraCmd);
            }
        }

        return dne;
    }
}
