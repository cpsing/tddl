package com.taobao.tddl.repo.mysql.handler;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.spi.ExecutionContext;
import com.taobao.tddl.executor.spi.Table;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.repo.mysql.spi.My_JdbcHandler;
import com.taobao.tddl.repo.mysql.spi.My_Log;

/**
 * @author mengshi.sunmengshi 2013-12-5 下午6:28:28
 * @since 5.1.0
 */
public class UpdateMyHandler extends PutMyHandlerCommon {

    public UpdateMyHandler(){
        super();
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected ISchematicCursor executePut(ExecutionContext executionContext, IPut put, Table table, IndexMeta meta,
                                          My_JdbcHandler myJdbcHandler) throws TddlException {
        if (put.getQueryTree() == null) {
            My_Log.getLog().warn("注意，做了全表更新操作");
        }
        myJdbcHandler.executeUpdate(context, executionContext, put, table, meta);
        return myJdbcHandler.getResultCursor();
    }
}
