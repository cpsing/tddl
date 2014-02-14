package com.taobao.tddl.qatest.matrix.select;

/**
 *  Copyright(c) 2010 taobao. All rights reserved.
 *  通用产品测试
 */

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.util.EclipseParameterized;
import com.taobao.tddl.qatest.ExecuteTableName;

/**
 * Comment for SelcetAmountLimit
 * <p/>
 * Author By: zhuoxue.yll Created Date: 2012-7-2 上午11:10:59
 */
@RunWith(EclipseParameterized.class)
public class SelectAmountLimit extends BaseMatrixTestCase {

    private final int AMOUNT_DATA = 100;
    private final int thread_size = 5;
    long              pk          = 1l;
    int               id          = 1;
    int               i           = 0;

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.normaltblTable(dbType));
    }

    public SelectAmountLimit(String normaltblTableName){
        BaseTestCase.normaltblTableName = normaltblTableName;
    }

    @Before
    public void MutilDataPrepare() throws SQLException {
        CountDownLatch latch = new CountDownLatch(thread_size);
        for (int i = 0; i < thread_size; i++) {
            Thread thread = new Thread(new InsertTask(latch), "Insert Task " + i);
            thread.start();
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
        }
    }

    @Test
    public void selectWithLimit() throws Exception {
        int start = 50;
        int limit = 10;
        String sql = "select * from " + normaltblTableName + " where name= ? limit ?,?";
        List<Object> param = new ArrayList<Object>();
        param.add(name);
        param.add(start);
        param.add(limit);
        rc = andorQueryData(sql, param);
        Assert.assertEquals(limit, resultsSize(rc));
    }

    @Test
    public void selectWithSelectLimit() throws Exception {
        int start = 5;
        int limit = 1;
        String sql = "select * from " + normaltblTableName + " as nor1 ,(select pk from " + normaltblTableName
                     + " limit ?,?) as nor2 where nor1.pk=nor2.pk";
        List<Object> param = new ArrayList<Object>();
        param.add(start);
        param.add(limit);
        rc = andorQueryData(sql, param);
        Assert.assertEquals(limit, resultsSize(rc));
    }

    private class InsertTask implements Runnable {

        private final CountDownLatch latch;

        public InsertTask(CountDownLatch latch){
            this.latch = latch;
        }

        @Override
        public void run() {
            String sql = "replace into " + normaltblTableName + "(pk,id,name) VALUES(?,?,?)";
            rc = null;
            List<Object> param = new ArrayList<Object>();
            for (; i < AMOUNT_DATA; i++) {
                param.clear();
                param.add(Long.parseLong(i + ""));
                param.add(i);
                param.add(name);
                try {
                    andorUpdateData(sql, param);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                try {
                    if (rc != null) rc.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            latch.countDown();
        }
    }

}
