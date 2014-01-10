package com.taobao.tddl.matrix.test;

import java.util.concurrent.Executors;

import org.junit.Test;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.MatrixExecutor;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ResultCursor;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.matrix.config.ConfigHolder;

public class ConfigHolderTest {

    @Test
    public void initTestWithConfigHolder() throws TddlException {

        ConfigHolder configHolder = new ConfigHolder();
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
        // ResultCursor rc = me.execute("select * from bmw_users limit 10",
        // context);
        ResultCursor rc = me.execute("select count(distinct gmt_create) as k  from mobile_pigeon_msg_log  order by gmt_create limit 10",
            context);

        IRowSet row = null;
        while ((row = rc.next()) != null) {
            System.out.println(row);
        }

        System.out.println("ok");
    }

}
