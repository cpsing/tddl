package com.taobao.tddl.optimizer.costbased.after;

import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;

/**
 * 防止死锁的thread 号选择器。 每层会独立指定一个thread进行执行。这样就算是A->B->A 也不会出现死锁的问题。
 * 出现死锁的主要原因是请求是blocking状态的，所以导致后面的流数据无法处理，死锁。
 * 如果要修复这个问题，必须将请求发送和请求回收处理，逻辑上分开。变成发送流和接受流。才能解决，目前的方案是权宜方案 不会改变结构
 * 
 * @author Whisper
 */
public class ChooseTreadOptimizer implements QueryPlanOptimizer {

    public ChooseTreadOptimizer(){
    }

    @Override
    public IDataNodeExecutor optimize(IDataNodeExecutor dne, Map<Integer, ParameterContext> parameterSettings,
                                      Map<String, Object> extraCmd) {

        if (extraCmd != null && extraCmd.get("initThread") != null) this.allocThread(dne,
            (Integer) extraCmd.get("initThread"));
        else {
            this.allocThread(dne, 0);
        }

        return dne;
    }

    private void allocThread(IDataNodeExecutor dne, int i) {
        dne.setThread(i);

        if (dne instanceof IPut) {
            if (((IPut) dne).getQueryTree() != null) {
                this.allocThread(((IPut) dne).getQueryTree(), i + 1);
            }

        } else if (dne instanceof IQuery) {
            if (((IQuery) dne).getSubQuery() != null) {
                this.allocThread(((IQuery) dne).getSubQuery(), i + 1);
            }

        } else if (dne instanceof IMerge) {
            for (IDataNodeExecutor sub : ((IMerge) dne).getSubNode()) {
                this.allocThread(sub, i + 1);
            }

        } else if (dne instanceof IJoin) {
            this.allocThread(((IJoin) dne).getLeftNode(), i + 1);
            this.allocThread(((IJoin) dne).getRightNode(), i + 1);
        }

    }

}
