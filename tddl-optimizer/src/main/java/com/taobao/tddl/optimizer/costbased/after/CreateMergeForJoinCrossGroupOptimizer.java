package com.taobao.tddl.optimizer.costbased.after;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.QueryPlanOptimizer;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;

/**
 * merge的mget优化
 * 
 * @author Whisper
 */
public class CreateMergeForJoinCrossGroupOptimizer implements QueryPlanOptimizer {

    @Override
    public IDataNodeExecutor optimize(IDataNodeExecutor dne, Map<Integer, ParameterContext> parameterSettings,
                                      Map<String, Comparable> extraCmd) {

        if (dne instanceof IMerge) {
            for (IDataNodeExecutor sub : ((IMerge) dne).getSubNode()) {
                this.optimize(sub, parameterSettings, extraCmd);
            }

        }
        if (dne instanceof IJoin) {
            this.optimize(((IJoin) dne).getLeftNode(), parameterSettings, extraCmd);
            this.optimize(((IJoin) dne).getRightNode(), parameterSettings, extraCmd);

            if (((IJoin) dne).getRightNode() instanceof IQuery) {
                IQuery right = (IQuery) ((IJoin) dne).getRightNode();

                if (right.getDataNode() != null) {
                    // right和join节点跨机，则需要右边生成Merge来做mget
                    if (!right.getDataNode().equals(dne.getDataNode())) {
                        IMerge merge = ASTNodeFactory.getInstance().createMerge();
                        merge.setAlias(((IQueryTree) right).getAlias());

                        List<ISelectable> mergeSelected = new ArrayList(((IQueryTree) right).getColumns().size());
                        for (Object s : ((IQueryTree) right).getColumns()) {
                            mergeSelected.add(((ISelectable) s).copy());
                        }

                        merge.setColumns(mergeSelected);
                        merge.setConsistent(right.getConsistent());
                        merge.setGroupBys(((IQueryTree) right).getGroupBys());
                        merge.setOrderBys(((IQueryTree) right).getOrderBys());
                        merge.executeOn(dne.getDataNode());
                        List<IDataNodeExecutor> subs = new ArrayList(0);

                        subs.add(right);

                        merge.setSubNode(subs);
                        merge.setSharded(false);
                        ((IJoin) dne).setRightNode(merge);
                    }

                }
            }
        }

        if (dne instanceof IQuery) {
            if (((IQuery) dne).getSubQuery() != null) this.optimize(dne, parameterSettings, extraCmd);
        }
        return dne;
    }

}
