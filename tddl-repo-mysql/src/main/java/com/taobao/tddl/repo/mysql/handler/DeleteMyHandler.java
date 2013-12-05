package com.taobao.ustore.jdbc.common.command.handler;

import java.util.Map;

import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.spi.ExecutionContext;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.repo.mysql.spi.My_JdbcHandler;

public class DeleteMyHandler extends PutMyHandlerCommon {

    public DeleteMyHandler(AndorContext commonConfig2){
        super(commonConfig2);
    }

    @Override
    protected ISchematicCursor executePut(Map<String, Comparable> context, ExecutionContext executionContext, IPut put,
                                          Table table, IndexMeta meta, My_JdbcHandler myJdbcHandler) throws Exception {
        myJdbcHandler.executeUpdate(context, executionContext, put, table, meta);
        return myJdbcHandler.getResultCursor();
    }
}
