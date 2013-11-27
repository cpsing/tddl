package com.taobao.tddl.optimizer.costbased.after;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import com.taobao.ustore.common.inner.bean.IDataNodeExecutor;
import com.taobao.ustore.common.inner.bean.IJoin;
import com.taobao.ustore.common.inner.bean.IMerge;
import com.taobao.ustore.common.inner.bean.IQuery;
import com.taobao.ustore.common.inner.bean.IQueryCommon;
import com.taobao.ustore.common.inner.bean.ParameterContext;
import com.taobao.ustore.optimizer.QueryPlanOptimizer;
import com.taobao.ustore.optimizer.util.RequestIDGen;

/**
 * 添加id 不会改变结构
 * 
 * @author Whisper
 */
public class FillRequestIDAndSubRequestID implements QueryPlanOptimizer {

    String hostname = "";

    public FillRequestIDAndSubRequestID(){
        InetAddress addr;
        try {
            addr = InetAddress.getLocalHost();
            String ip = addr.getHostAddress().toString();

            hostname = ip + System.currentTimeMillis();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

    }

    @Override
    public IDataNodeExecutor optimize(IDataNodeExecutor dne, Map<Integer, ParameterContext> parameterSettings,
                                      Map<String, Comparable> extraCmd) {
        if (dne instanceof IQueryCommon) {
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
