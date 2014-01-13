package com.taobao.tddl.optimizer.costbased.after;

import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.utils.AddressUtils;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.optimizer.utils.RequestIDGen;

/**
 * 添加id 不会改变结构
 * 
 * @author Whisper
 */
public class FillRequestIDAndSubRequestID implements QueryPlanOptimizer {

    String hostname = "";

    public FillRequestIDAndSubRequestID(){
        hostname = AddressUtils.getHostIp() + "_" + System.currentTimeMillis();
    }

    @Override
    public IDataNodeExecutor optimize(IDataNodeExecutor dne, Map<Integer, ParameterContext> parameterSettings,
                                      Map<String, Object> extraCmd) {
        if (dne instanceof IQueryTree) {
            fillRequestIDAndSubRequestIDFromRoot(dne, 1);
        } else {
            dne.setSubRequestID(1);
            dne.setRequestID(RequestIDGen.genRequestID());
            dne.setRequestHostName(hostname);
        }
        return dne;
    }

    public int fillRequestIDAndSubRequestIDFromRoot(IDataNodeExecutor qc, int subRequestID) {
        qc.setSubRequestID(subRequestID);
        qc.setRequestID(RequestIDGen.genRequestID());
        qc.setRequestHostName(hostname);

        if (qc instanceof IQuery && ((IQuery) qc).getSubQuery() != null) {
            subRequestID = this.fillRequestIDAndSubRequestIDFromRoot(((IQuery) qc).getSubQuery(), subRequestID + 1);
        } else if (qc instanceof IMerge) {
            for (IDataNodeExecutor sub : ((IMerge) qc).getSubNode()) {
                subRequestID = this.fillRequestIDAndSubRequestIDFromRoot(sub, subRequestID + 1);
            }
        } else if (qc instanceof IJoin) {
            subRequestID = this.fillRequestIDAndSubRequestIDFromRoot(((IJoin) qc).getLeftNode(), subRequestID + 1);
            subRequestID = this.fillRequestIDAndSubRequestIDFromRoot(((IJoin) qc).getRightNode(), subRequestID + 1);
        }

        return subRequestID;
    }

}
