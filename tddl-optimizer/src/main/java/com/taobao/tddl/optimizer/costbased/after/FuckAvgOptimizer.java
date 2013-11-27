package com.taobao.tddl.optimizer.costbased.after;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.taobao.ustore.common.inner.bean.Function;
import com.taobao.ustore.common.inner.bean.IDataNodeExecutor;
import com.taobao.ustore.common.inner.bean.IFunction;
import com.taobao.ustore.common.inner.bean.IJoin;
import com.taobao.ustore.common.inner.bean.IMerge;
import com.taobao.ustore.common.inner.bean.IQuery;
import com.taobao.ustore.common.inner.bean.IQueryCommon;
import com.taobao.ustore.common.inner.bean.ISelectable;
import com.taobao.ustore.common.inner.bean.ParameterContext;
import com.taobao.ustore.common.inner.bean.IFunction.FunctionType;
import com.taobao.ustore.optimizer.QueryPlanOptimizer;

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
                    for (ISelectable s : ((IQueryCommon) sub).getColumns()) {
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
                        for (ISelectable s : ((IQueryCommon) sub).getColumns()) {
                            if (s instanceof IFunction) {
                                if (FunctionType.Scalar.equals(((IFunction) s).getFunctionType())) remove.add(s);
                                else if (s.getColumnName().startsWith("AVG(")) {
                                    Function count = (Function) s.copy();
                                    count.setiExtraFunctionCache(null);
                                    count.setFunctionName("COUNT");
                                    count.setColumnName(s.getColumnName().replace("AVG(", "COUNT("));

                                    Function sum = (Function) s.copy();
                                    sum.setiExtraFunctionCache(null);
                                    sum.setFunctionName("SUM");
                                    sum.setColumnName(s.getColumnName().replace("AVG(", "SUM("));
                                    add.add(count);
                                    add.add(sum);

                                    remove.add(s);
                                }
                            }
                        }

                        if (!remove.isEmpty()) {
                            ((IQueryCommon) sub).getColumns().removeAll(remove);
                            ((IQueryCommon) sub).getColumns().addAll(add);
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
