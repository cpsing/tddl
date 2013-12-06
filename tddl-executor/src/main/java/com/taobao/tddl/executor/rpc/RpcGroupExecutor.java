package com.taobao.tddl.executor.rpc;

import java.util.concurrent.Future;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.spi.IGroupExecutor;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.optimizer.config.Group;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

/**
 * rpc的，暂未实现
 * 
 * @author mengshi.sunmengshi 2013-12-6 下午2:21:36
 * @since 5.1.0
 */
public class RpcGroupExecutor implements IGroupExecutor {

    @Override
    public Future<ISchematicCursor> commitFuture(ExecutionContext executionContext) throws TddlException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Future<ISchematicCursor> rollbackFuture(ExecutionContext executionContext) throws TddlException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Future<ISchematicCursor> execByExecPlanNodeFuture(IDataNodeExecutor qc, ExecutionContext executionContext)
                                                                                                                     throws TddlException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void commit(ExecutionContext executionContext) throws TddlException {
        // TODO Auto-generated method stub

    }

    @Override
    public void rollback(ExecutionContext executionContext) throws TddlException {
        // TODO Auto-generated method stub

    }

    @Override
    public ISchematicCursor execByExecPlanNode(IDataNodeExecutor qc, ExecutionContext executionContext)
                                                                                                       throws TddlException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Group getGroupInfo() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object getRemotingExecutableObject() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IRepository getRepository() {
        // TODO Auto-generated method stub
        return null;
    }

}
