package com.taobao.tddl.hint.test;

import java.util.concurrent.Executors;

import org.junit.Test;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.MatrixExecutor;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ResultCursor;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.matrix.config.MatrixConfigHolder;
import com.taobao.tddl.optimizer.core.plan.bean.DataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.bean.Merge;
import com.taobao.tddl.optimizer.core.plan.bean.Query;

public class HintExecuteTest {

    @Test
    public void initTestWithConfigHolder() throws TddlException {
        MatrixConfigHolder configHolder = new MatrixConfigHolder();
        configHolder.setAppName("andor_show");
        configHolder.setTopologyFilePath("test_matrix.xml");
        configHolder.setSchemaFilePath("test_schema.xml");

        try {
            configHolder.init();
        } catch (TddlException e) {
            e.printStackTrace();
        }

        MatrixExecutor me = new MatrixExecutor();

        ExecutionContext context = new ExecutionContext();
        context.setExecutorService(Executors.newCachedThreadPool());

        Merge merge = new Merge();
        DataNodeExecutor sub1 = new Query();
        sub1.setSql("select * from bmw_users_0003 limit 10");
        sub1.executeOn("andor_show_group0");

        DataNodeExecutor sub2 = new Query();
        sub2.setSql("select * from bmw_users_0001 limit 10");
        sub2.executeOn("andor_show_group0");

        merge.addSubNode(sub1);
        merge.addSubNode(sub2);
        merge.setSql("sql");
        merge.executeOn("andor_show_group0");

        ResultCursor rc = me.execByExecPlanNode(merge, context);
        IRowSet row = null;
        while ((row = rc.next()) != null) {
            System.out.println(row);
        }

        System.out.println("ok");
    }
}
