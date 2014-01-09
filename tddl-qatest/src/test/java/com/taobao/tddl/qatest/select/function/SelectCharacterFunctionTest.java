package com.taobao.tddl.qatest.select.function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseTddlTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.util.EclipseParameterized;
import com.taobao.tddl.qatest.util.ExecuteTableName;

@RunWith(EclipseParameterized.class)
public class SelectCharacterFunctionTest extends BaseTddlTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.normaltblTable(dbType));
    }

    public SelectCharacterFunctionTest(String tableName){
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

    @Test
    public void concatTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where name =concat(?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add("zhuo");
        param.add("xue");
        String[] columnParam = { "ID", "GMT_CREATE", "NAME", "FLOATCOL", "GMT_TIMESTAMP", "GMT_DATETIME" };
        selectContentSameAssert(sql, columnParam, param);
        sql = "select * from " + normaltblTableName + " where name like concat (?,?,?)";
        param.clear();
        param.add("zhu");
        param.add("o");
        param.add("xue");
        selectContentSameAssert(sql, columnParam, param);
    }

    /**
     * concat加上任意参数
     */
    @Test
    public void concatArbitrarilyTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where name =concat(?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add("%uo");
        param.add("xu_");
        String[] columnParam = { "ID", "GMT_CREATE", "NAME", "FLOATCOL", "GMT_TIMESTAMP", "GMT_DATETIME" };
        selectContentSameAssert(sql, columnParam, param);
    }

    @Test
    public void ifnullTest() throws Exception {
        String sql = "select ifnull(name,pk) as notNullName from " + normaltblTableName;
        String[] columnParam = { "notNullName" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "replace into " + normaltblTableName + " (pk,name) values (10,null)";
        execute(sql, null);

        sql = "select ifnull(name,'ni') as notNullName from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select ifnull(name,'pk') as notNullName from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Ignore
    @Test
    public void ifnullTestTypeNotSame() throws Exception {
        String sql = "select ifnull(name,pk) as notNullName from " + normaltblTableName;
        String[] columnParam = { "notNullName" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void ifnullRoundTest() throws Exception {
        String sql = "replace into " + normaltblTableName + " (pk,floatCol) values (10,null)";
        execute(sql, null);
        sql = "select ifnull(round(floatCol/4,4),0) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void quoteTest() throws Exception {
        String sql = String.format("replace into %s (pk,name) values (10,quote(?))", normaltblTableName);
        List<Object> param = new ArrayList<Object>();
        param.add("'zhuoxue'");
        execute(sql, param);

        sql = String.format("select * from %s where name =quote(?)", normaltblTableName);
        String[] columnParam = { "ID", "GMT_CREATE", "NAME", "FLOATCOL", "GMT_TIMESTAMP", "GMT_DATETIME" };
        selectContentSameAssert(sql, columnParam, param);
    }

    @Test
    public void convTest() throws Exception {
        String sql = String.format("select conv(id,16,2) as a from %s where pk=1", normaltblTableName);
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, null);
    }

    @Test
    public void asciiTest() throws Exception {
        String sql = String.format("select ASCII(name) as a from %s", normaltblTableName);
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, null);
    }

    @Test
    public void bit_lengthTest() throws Exception {
        String sql = String.format("select BIT_LENGTH(name) as a from %s", normaltblTableName);
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, null);
    }

    @Test
    public void bitTest() throws Exception {
        String sql = String.format("select bin(name) as a from %s", normaltblTableName);
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, null);
    }

}
