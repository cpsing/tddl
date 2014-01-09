package com.taobao.tddl.repo.bdb.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.Group;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.ResultCursor;
import com.taobao.tddl.executor.spi.IAtomExecutor;
import com.taobao.tddl.executor.spi.IGroupExecutor;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

@SuppressWarnings("rawtypes")
public class BDBGroupExecutor extends AbstractLifecycle implements IGroupExecutor {

    private final ArrayList<IAtomExecutor> executorList;

    private ResourceSelector               slaveSelector;
    private ExceptionSorter<Integer>       slaveExceptionSorter;

    private ResourceSelector               masterSelector;
    private ExceptionSorter<Integer>       masterExceptionSorter;

    /**
     * @param resultSetMap 结果集对应Map
     * @param executorList 执行client的list
     * @param rWeight 读权重
     */
    public BDBGroupExecutor(ArrayList<IAtomExecutor> executorList, List<Integer> rWeight){
        this.executorList = executorList;
        this.masterExceptionSorter = new MasterExceptionSorter<Integer>();
        this.slaveExceptionSorter = new SlaveExceptionSorter<Integer>();
        this.masterSelector = new MasterResourceSelector(executorList.size());
        this.slaveSelector = new SlaveResourceSelector(executorList.size(), rWeight);
    }

    private static int findIndexToExecute(ResourceSelector selector, Map<Integer, String> excludeKeys)
                                                                                                      throws TddlException {
        int index = 0;
        Integer specIndex = null;

        if (specIndex != null) {
            index = specIndex;
        } else {
            index = selector.select(excludeKeys);
        }
        return index;
    }

    @Override
    public String toString() {
        return "BDBGroupExecutor [masterSelector=" + masterSelector + "]";
    }

    public ResourceSelector getSlaveSelector() {
        return slaveSelector;
    }

    public void setSlaveSelector(ResourceSelector slaveSelector) {
        this.slaveSelector = slaveSelector;
    }

    public ExceptionSorter<Integer> getSlaveExceptionSorter() {
        return slaveExceptionSorter;
    }

    public void setSlaveExceptionSorter(ExceptionSorter<Integer> slaveExceptionSorter) {
        this.slaveExceptionSorter = slaveExceptionSorter;
    }

    public ResourceSelector getMasterSelector() {
        return masterSelector;
    }

    public void setMasterSelector(ResourceSelector masterSelector) {
        this.masterSelector = masterSelector;
    }

    public ExceptionSorter<Integer> getMasterExceptionSorter() {
        return masterExceptionSorter;
    }

    public void setMasterExceptionSorter(ExceptionSorter<Integer> masterExceptionSorter) {
        this.masterExceptionSorter = masterExceptionSorter;
    }

    @Override
    public ISchematicCursor execByExecPlanNode(IDataNodeExecutor qc, ExecutionContext executionContext)
                                                                                                       throws TddlException {

        // IAtomExecutor atomExecutor = this.selectAtomExecutor(executeType,
        // executionContext);
        // try {
        // rsHandler = commandExecutor.execByExecPlanNode(qc, executionContext);
        // } catch (TddlException e) {
        // ReturnVal<Integer> isRetryException =
        // exceptionSorter.isRetryException(e.getMessage(), excludeKeys, index);
        // excludeKeys = isRetryException.getExcludeKeys();
        // if (!isRetryException.isRetryException()) {
        // throw e;
        // } else {
        // // 将excludeKey 加入map后，进入下一次循环
        // continue;
        // }
        // }

        return null;

    }

    enum ExecuteType {
        READ, WRITE
    };

    IAtomExecutor selectAtomExecutor(ExecuteType executeType, ExecutionContext executionContext) throws TddlException {
        Map<Integer, String/* exception */> excludeKeys = null;
        IAtomExecutor atomExecutor = null;
        ResourceSelector selector = null;

        if (executeType == ExecuteType.READ) {
            selector = this.getSlaveSelector();

        } else {
            selector = this.getMasterSelector();

        }
        int index = findIndexToExecute(selector, excludeKeys);

        atomExecutor = executorList.get(index);

        return atomExecutor;
    }

    @Override
    public ResultCursor commit(ExecutionContext executionContext) throws TddlException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultCursor rollback(ExecutionContext executionContext) throws TddlException {
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
    public Future<ResultCursor> commitFuture(ExecutionContext executionContext) throws TddlException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Future<ResultCursor> rollbackFuture(ExecutionContext executionContext) throws TddlException {
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

}
