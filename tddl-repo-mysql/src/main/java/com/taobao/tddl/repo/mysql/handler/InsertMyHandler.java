package com.taobao.tddl.repo.mysql.handler;

import java.util.Map;

import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.spi.ExecutionContext;
import com.taobao.tddl.executor.spi.Table;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.repo.mysql.spi.My_JdbcHandler;

/**
 * @author mengshi.sunmengshi 2013-12-5 下午6:19:17
 * @since 5.1.0
 */
public class InsertMyHandler extends PutMyHandlerCommon {

    public InsertMyHandler(){
        super();
    }

    @Override
    protected ISchematicCursor executePut(Map<String, Comparable> context, ExecutionContext executionContext, IPut put,
                                          Table table, IndexMeta meta, My_JdbcHandler myJdbcHandler) throws Exception {
        myJdbcHandler.executeUpdate(context, executionContext, put, table, meta);
        return myJdbcHandler.getResultCursor();
    }

}
