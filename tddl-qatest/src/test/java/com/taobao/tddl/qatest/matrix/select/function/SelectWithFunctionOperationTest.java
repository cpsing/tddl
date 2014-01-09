package com.taobao.tddl.qatest.matrix.select.function;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.util.ExecuteTableName;

@RunWith(Parameterized.class)
public class SelectWithFunctionOperationTest extends BaseMatrixTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.normaltblTable(dbType));
    }

    public SelectWithFunctionOperationTest(String tableName){
        BaseTestCase.normaltblTableName = tableName;
    }

    @Before
    public void prepare() throws Exception {
        con = getConnection();
        andorCon = us.getConnection();
        normaltblPrepare(0, 20);
    }

    @After
    public void destory() throws Exception {
        psConRcRsClose(rc, rs);
    }

    /**
     * 函数之间的加减操作
     * 
     * @throws Exception
     */
    @Test
    public void testFunctionPlusMusTest() throws Exception {
        String sql = "SELECT MAX(pk)+MIN(pk)  as a FROM " + normaltblTableName;
        String[] columnParam = { "a" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "SELECT MAX(pk)+MIN(pk) as plus FROM " + normaltblTableName;
        String[] columnParamp = { "plus" };
        selectOrderAssert(sql, columnParamp, Collections.EMPTY_LIST);

        sql = "SELECT MAX(pk)-MIN(pk) as a FROM " + normaltblTableName;
        String[] columnParam1 = { "a" };
        selectOrderAssert(sql, columnParam1, Collections.EMPTY_LIST);

        sql = "SELECT MAX(pk)-MIN(pk) mus FROM " + normaltblTableName;
        String[] columnParamm = { "mus" };
        selectOrderAssert(sql, columnParamm, Collections.EMPTY_LIST);

        sql = "SELECT MAX(pk)--MIN(pk) mus FROM " + normaltblTableName;
        selectOrderAssert(sql, columnParamm, Collections.EMPTY_LIST);
    }

    /**
     * 函数之间的乘除操作
     * 
     * @throws Exception
     */
    @Test
    public void testFunctionMultiplicationDivisionTest() throws Exception {
        String sql = "SELECT sum(pk)/max(id)  as a FROM " + normaltblTableName;
        String[] columnParam = { "a" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "SELECT sum(pk)/count(*) as Division FROM " + normaltblTableName;
        String[] columnParamp = { "Division" };
        selectOrderAssert(sql, columnParamp, Collections.EMPTY_LIST);

        sql = "SELECT max(id)*count(id) as c FROM " + normaltblTableName;
        String[] columnParam1 = { "c" };
        selectOrderAssert(sql, columnParam1, Collections.EMPTY_LIST);

        sql = "SELECT avg(id)*count(*) Multiplication FROM " + normaltblTableName;
        String[] columnParamm = { "Multiplication" };
        selectOrderAssert(sql, columnParamm, Collections.EMPTY_LIST);
    }

    @Ignore("暂时不支持mod的merge实现")
    @Test
    public void testFunctionModTest() throws Exception {
        String sql = "SELECT sum(pk)%count(*) sd FROM " + normaltblTableName;
        String[] columnParamm2 = { "sd" };
        selectOrderAssert(sql, columnParamm2, Collections.EMPTY_LIST);
    }

    @Test
    public void testFunctionPlus() throws Exception {
        String sql = "SELECT pk FROM " + normaltblTableName + " where pk=1+1";
        String[] columnParamm2 = { "pk" };
        selectOrderAssert(sql, columnParamm2, Collections.EMPTY_LIST);
    }

    /**
     * 函数之间的乘除操作
     * 
     * @throws Exception
     */
    @Test
    public void testFunctionMixTest() throws Exception {

        String sql = "SELECT sum(pk)/max(id)+count(*) as b  FROM " + normaltblTableName;
        String[] columnParam = { "b" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "SELECT max(id)*count(id)/8-count(*)*2+min(id) as c FROM " + normaltblTableName;
        String[] columnParam1 = { "c" };
        selectOrderAssert(sql, columnParam1, Collections.EMPTY_LIST);
    }

}
