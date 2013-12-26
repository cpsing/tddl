package com.taobao.tddl.qatest.select;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseAndorTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.ExecuteTableName;
import com.taobao.tddl.qatest.Validator;

/**
 * @author zhuoxue.yll 2013.01.23
 */
@RunWith(Parameterized.class)
public class SelectWithNullTest extends BaseAndorTestCase {

    String[] columnParam = { "PK", "NAME", "ID" };

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.normaltblTable(dbType));
    }

    public SelectWithNullTest(String tableName){
        BaseTestCase.normaltblTableName = tableName;
    }

    @Before
    public void prepareDate() throws Exception {
        Validator.con = Validator.getConnection();
        Validator.andorCon = us.getConnection();
        prepareData.normaltblNullPrepare(0, 20);
    }

    @After
    public void clearDate() throws Exception {
        Validator.psConRcRsClose(rc, rs);
    }

    @Test
    public void isNull() throws Exception {
        String sql = "select * from " + normaltblTableName + " where name is ?";
        List<Object> param = new ArrayList<Object>();
        param.add(null);
        selectContentSameAssert(sql, columnParam, param);

        sql = "select * from " + normaltblTableName + " where name is null";
        selectContentSameAssert(sql, columnParam, null);
    }

    @Test
    public void isNotNull() throws Exception {
        String sql = "select * from " + normaltblTableName + " where name is not ?";
        List<Object> param = new ArrayList<Object>();
        param.add(null);
        selectContentSameAssert(sql, columnParam, param);

        sql = "select * from " + normaltblTableName + " where name is not null";
        selectContentSameAssert(sql, columnParam, null);
    }

    /**
     * 等于null，mysql查询不出结果
     */
    @Test
    public void equalNullTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where name = ?";
        List<Object> param = new ArrayList<Object>();
        param.add(null);
        selectContentSameAssert(sql, columnParam, param);

        sql = "select * from " + normaltblTableName + " where name =null";
        selectContentSameAssert(sql, columnParam, null);
    }

    public void quoteNullTest() throws Exception {
        String sql = "select QUOTE(?) a from " + normaltblTableName;
        String[] columnParam = { "a" };
        List<Object> param = new ArrayList<Object>();
        param.add(null);
        selectContentSameAssert(sql, columnParam, param);

        sql = "select QUOTE(null) a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, null);
    }

    @Test
    public void asciiNULLTest() throws Exception {
        String sql = String.format("select ASCII(name) as a from %s", normaltblTableName);
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, null);
    }
}
