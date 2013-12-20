package com.taobao.tddl.executor.handler;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;

public class NestedLoopJoinHandler extends QueryHandlerCommon {

    public NestedLoopJoinHandler(){
        super();
    }

    @Override
    protected ISchematicCursor doQuery(ISchematicCursor cursor, IDataNodeExecutor executor,
                                       ExecutionContext executionContext) throws TddlException {

        IJoin join = (IJoin) executor;
        IRepository repo = executionContext.getCurrentRepository();
        IQueryTree leftQuery = join.getLeftNode();

        ISchematicCursor cursor_left = ExecutorContext.getContext()
            .getTopologyExecutor()
            .execByExecPlanNode(leftQuery, executionContext);

        ISchematicCursor cursor_right = ExecutorContext.getContext()
            .getTopologyExecutor()
            .execByExecPlanNode(join.getRightNode(), executionContext);

        cursor = repo.getCursorFactory().blockNestedLoopJoinCursor(executionContext,
            cursor_left,
            cursor_right,
            join.getLeftJoinOnColumns(),
            join.getRightJoinOnColumns(),
            join.getColumns(),
            join);
        return cursor;
    }

}
