package com.taobao.tddl.optimizer.costbased.after;

import java.util.Map;

import com.taobao.ustore.common.inner.bean.IColumn;
import com.taobao.ustore.common.inner.bean.IDataNodeExecutor;
import com.taobao.ustore.common.inner.bean.IJoin;
import com.taobao.ustore.common.inner.bean.IMerge;
import com.taobao.ustore.common.inner.bean.IQuery;
import com.taobao.ustore.common.inner.bean.IQueryCommon;
import com.taobao.ustore.common.inner.bean.ISelectable;
import com.taobao.ustore.common.inner.bean.ParameterContext;
import com.taobao.ustore.common.inner.bean.IParallelizableQueryCommon.QUERY_CONCURRENCY;
import com.taobao.ustore.optimizer.QueryPlanOptimizer;

public class MergeColumnsOptimizer implements QueryPlanOptimizer {

    public MergeColumnsOptimizer(){
    }

    /**
     * 将merge子节点中的orderby，groupby列加入到子节点的columns中，如果没有的话
     */
    @Override
    public IDataNodeExecutor optimize(IDataNodeExecutor dne, Map<Integer, ParameterContext> parameterSettings,
                                      Map<String, Comparable> extraCmd) {

        if (dne instanceof IQueryCommon) {
            this.findMergeAndAddTempColumnsInChild(dne);
        }

        return dne;
    }

    void findMergeAndAddTempColumnsInChild(IDataNodeExecutor dne) {
        if (dne instanceof IMerge) {
            ((IMerge) dne).setQueryConcurrency(QUERY_CONCURRENCY.SEQUENTIAL);

            for (IDataNodeExecutor child : ((IMerge) dne).getSubNode()) {
                if (child instanceof IQueryCommon) {
                    for (ISelectable s : ((IQueryCommon) child).getColumns()) {
                        s.setAlias(null);

                        if (s instanceof IColumn) {
                            ((IQueryCommon) child).setAlias(s.getTableName());
                        }

                    }
                    //索引的查询必须有别名，其它的可以没有
                    if (!(child instanceof IQuery && ((IQuery) child).getSubQuery() == null)) {
                        ((IQueryCommon) child).setAlias(null);
                    }
                }
                this.findMergeAndAddTempColumnsInChild(child);
            }
        }

        if (dne instanceof IJoin) {
            this.findMergeAndAddTempColumnsInChild(((IJoin) dne).getLeftNode());
            this.findMergeAndAddTempColumnsInChild(((IJoin) dne).getRightNode());
        }

        if (dne instanceof IQuery && ((IQuery) dne).getSubQuery() != null) {
            this.findMergeAndAddTempColumnsInChild(((IQuery) dne).getSubQuery());

        }

    }
}
