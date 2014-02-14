package com.taobao.tddl.qatest.matrix.select;

/**
 *  Copyright(c) 2010 taobao. All rights reserved.
 *  通用产品测试
 */

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
public class SelectMinusTest extends BaseMatrixTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.normaltblTable(dbType));
    }

    public SelectMinusTest(String normaltblTableName){
        BaseTestCase.normaltblTableName = normaltblTableName;
    }

    @Before
    public void MutilDataPrepare() throws Exception {
        normaltblPrepare(-10, 20);
    }

    @Test
    public void cloumMinusTest() throws Exception {
        String sql = String.format("select -id as a from %s where name=?", normaltblTableName);
        List<Object> param = new ArrayList<Object>();
        param.add(name);
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, param);

        sql = String.format("select pk-id as a from %s where name=?", normaltblTableName);
        selectContentSameAssert(sql, columnParam, param);

        sql = String.format("select pk-(-id) as a from %s where name=?", normaltblTableName);
        selectContentSameAssert(sql, columnParam, param);

        // sql = String.format("select pk---id as a from %s where name=?",
        // normaltblTableName);
        // selectContentSameAssert(sql, columnParam, param);

        // sql = String.format("select pk--(-id) as a from %s where name=?",
        // normaltblTableName);
        // selectContentSameAssert(sql, columnParam, param);
    }

    @Test
    public void conditionMinusTest() throws Exception {
        String sql = String.format("select * from %s where id<pk-?", normaltblTableName);
        List<Object> param = new ArrayList<Object>();
        param.add(50);
        String[] columnParam = { "PK", "ID", "GMT_CREATE", "NAME", "FLOATCOL", "GMT_TIMESTAMP", "GMT_DATETIME" };
        selectContentSameAssert(sql, columnParam, param);

        // sql = String.format("select * from %s where pk<id--?",
        // normaltblTableName);
        // selectContentSameAssert(sql, columnParam, param);
        //
        // sql = String.format("select * from %s where pk<id---?",
        // normaltblTableName);
        // selectContentSameAssert(sql, columnParam, param);
        //
        // sql = String.format("select * from %s where pk<id---(-?)",
        // normaltblTableName);
        // selectContentSameAssert(sql, columnParam, param);
    }
}
