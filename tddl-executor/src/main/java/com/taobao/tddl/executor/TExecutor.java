package com.taobao.tddl.executor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.common.model.lifecycle.Lifecycle;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.executor.cursor.ResultCursor;
import com.taobao.tddl.executor.cursor.impl.QueryPlanResultCursor;
import com.taobao.tddl.executor.exception.DataAccessException;
import com.taobao.tddl.executor.spi.ExecutionContext;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;

public class TExecutor implements Lifecycle, ITransactionAsyncExecutor, ITransactionExecutor {

    private final static Logger logger = LoggerFactory.getLogger(TExecutor.class);

    private boolean             inited = false;

    /**
     * client端核心流程 解析 优化 执行
     * 
     * @param extraCmd
     * @param sql
     * @param context
     * @param createTxn
     * @param transactionSequence
     * @param closeResultSet
     * @return
     * @throws DataAccessException
     */
    public ResultCursor execute(String sql,

    ExecutionContext executionContext) throws TddlException {

        if (logger.isDebugEnabled()) {
            logger.warn("extraCmd:\n" + executionContext.getExtraCmds());
            logger.warn("ParameterContext:\n" + executionContext.getParams());
        }

        // client端核心流程y
        try {
            long time = System.currentTimeMillis();

            int explainIndex = explainIndex(sql);

            if (explainIndex > 0) {
                sql = sql.substring(explainIndex);
            }
            IDataNodeExecutor qc = parseAndOptimize(sql, executionContext);
            // System.out.println(qc);
            if (explainIndex > 0) {
                return createQueryPlanResultCursor(qc);
            }

            boolean useTemporaryTable = clientContext.getUserConfig().useTemporaryTable();
            extraCmd = processTemporary(extraCmd, useTemporaryTable);

            ResultCursor c = clientContext.getMatrixExecutor()
                .get()
                .execute(extraCmd, createTxn, transactionSequence, qc, closeResultSet, executionContext);

            // 控制语句
            time = GeneralUtil.monitorAndRenewTime(Key1, AndOrExecutorExecute, Key3Success, time);

            if (qc instanceof IQueryTree) {
                // 做下表名替换
                List columnsForResultSet = ((IQueryTree) qc).getColumns();
                if (((IQueryTree) qc).getAlias() != null) {
                    columnsForResultSet = ExecUtils.copySelectables(columnsForResultSet);
                    for (Object s : columnsForResultSet) {
                        ((ISelectable) s).setTableName(((IQueryTree) qc).getAlias());
                    }
                }
                c.setOriginalSelectColumns(columnsForResultSet);
            }
            return c;
        } catch (EmptyResultRestrictionException e) {
            // e.printStackTrace();
            return ResultCursor.EMPTY_RESULT_CURSOR;
        } catch (Exception e) {
            throw new DataAccessException(e);
        }
    }

    private ResultCursor createQueryPlanResultCursor(IDataNodeExecutor qc) {
        return new QueryPlanResultCursor(qc.toString(), null);
    }

    public static final String EXPLAIN = "explain";

    private int explainIndex(String sql) {
        String temp = sql;
        int i = 0;
        for (; i < temp.length(); ++i) {
            switch (temp.charAt(i)) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    continue;
            }
            break;
        }

        if (temp.toLowerCase().startsWith(EXPLAIN, i)) {
            return i + EXPLAIN.length();
        } else {
            return -1;
        }
    }

    private Map<String, Comparable> processTemporary(Map<String, Comparable> extraCmd, boolean useTemparyTable) {
        if (useTemparyTable) {
            if (extraCmd == null) {
                extraCmd = new HashMap<String, Comparable>(2);
            }
            Comparable comp = extraCmd.get(ExtraCmd.ExecutionExtraCmd.ALLOW_TEMPORARY_TABLE);
            if (comp == null) {
                extraCmd.put(ExtraCmd.ExecutionExtraCmd.ALLOW_TEMPORARY_TABLE, "TRUE");
            }
        }
        return extraCmd;
    }

    public IDataNodeExecutor parseAndOptimize(String sql, ExecutionContext executionContext) throws TddlException {

        boolean cache = false;

        if ("True".equalsIgnoreCase(GeneralUtil.getExtraCmd(executionContext.getExtraCmds(),
            ExtraCmd.ConnectionExtraCmd.OPTIMIZER_CACHE))) cache = true;
        IDataNodeExecutor qc = OptimizerContext.getContext()
            .getOptimizer()
            .optimizeAndAssignment(sql, executionContext.getParams(), executionContext.getExtraCmds(), cache);

        return qc;
    }

    @Override
    public void init() throws TddlException {
        // TODO Auto-generated method stub

    }

    @Override
    public void destory() throws TddlException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isInited() {
        return this.inited;
    }

    @Override
    public Future<ResultCursor> execByExecPlanNodeFuture(IDataNodeExecutor qc, ExecutionContext executionContext)
                                                                                                                 throws TddlException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResultCursor execByExecPlanNode(IDataNodeExecutor qc, ExecutionContext executionContext)
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
    public Future<ResultCursor> commitFuture(ExecutionContext executionContext) throws TddlException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Future<ResultCursor> rollbackFuture(ExecutionContext executionContext) throws TddlException {
        // TODO Auto-generated method stub
        return null;
    }

}
