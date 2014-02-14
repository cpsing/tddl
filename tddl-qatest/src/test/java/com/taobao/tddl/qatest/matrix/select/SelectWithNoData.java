package com.taobao.tddl.qatest.matrix.select;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
 * Comment for SelectWithNoData
 * <p/>
 * Author By: zhuoxue.yll Created Date: 2012-11-12 下午05:26:41
 */
@RunWith(EclipseParameterized.class)
public class SelectWithNoData extends BaseMatrixTestCase {

    long pk = 1l;
    int  id = 1;

    @Parameters(name = "{index}:table0={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableName.normaltblTable(dbType));
    }

    public SelectWithNoData(String normaltblTableName){
        BaseTestCase.normaltblTableName = normaltblTableName;
    }

    @Before
    public void prepareData() throws Exception {
        andorUpdateData("delete from  " + normaltblTableName, null);
        mysqlUpdateData("delete from  " + normaltblTableName, null);
    }

    @Test
    public void aliasTest() throws Exception {
        String sql = "select name as xingming ,id as pid from  " + normaltblTableName + "  as nor where pk=?";
        List<Object> param = new ArrayList<Object>();
        param.add(pk);
        String[] columnParam = { "xingming", "pid" };
        assertAlias(sql, columnParam, "nor", param);
    }

    @Test
    public void OrWithDifFiledTest() throws Exception {
        long pk = 2l;
        int id = 3;

        String sql = "select * from " + normaltblTableName + " where pk= ? or id=?";
        List<Object> param = new ArrayList<Object>();
        param.add(pk);
        param.add(id);
        String[] columnParam = { "name", "pk", "id" };
        selectContentSameAssert(sql, columnParam, param);
    }

    @Test
    public void conditionWithGreaterTest() throws Exception {
        String[] columnParam = { "PK", "NAME", "ID" };
        String sql = "select * from " + normaltblTableName + " where pk>? order by pk";
        List<Object> param = new ArrayList<Object>();
        param.add(Long.parseLong(0 + ""));
        selectOrderAssert(sql, columnParam, param);

        sql = "select * from " + normaltblTableName + " where pk>=? order by pk";
        param.clear();
        param.add(Long.parseLong(0 + ""));
        selectOrderAssert(sql, columnParam, param);
    }

    @Test
    public void InTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk in (1,2,3)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void LikeAnyTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where name like 'zhuo%'";
        String[] columnParam = { "ID", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select * from " + normaltblTableName + " where name like '%uo%'";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select * from " + normaltblTableName + " where name like '%uo%u%'";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void OrderByAscTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where name= ? order by id asc";
        List<Object> param = new ArrayList<Object>();
        param.add(name);
        String[] columnParam = { "PK", "ID", "NAME" };
        selectOrderAssertNotKeyCloumn(sql, columnParam, param, "id");
    }

    @Test
    public void GroupByTest() throws Exception {
        String sql = "select count(pk) ,name as n from  " + normaltblTableName + "   group by n";
        String[] columnParam = { "count(pk)", "n" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void LimitWithStart() throws Exception {
        int start = 5;
        int limit = 6;
        String sql = "SELECT * FROM " + normaltblTableName + " order by pk LIMIT " + start + "," + limit;
        String[] columnParam = { "name", "pk", "id" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

}
