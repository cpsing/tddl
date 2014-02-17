package com.taobao.tddl.qatest.matrix.select;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.ExecuteTableName;
import com.taobao.tddl.qatest.util.EclipseParameterized;

/**
 * 主要针对合并约束条件的测试 Author By: zhuoxue.yll Created Date: 2012-9-14 下午02:35:55
 */
@RunWith(EclipseParameterized.class)
public class SelectComplexConditionTest extends BaseMatrixTestCase {

    String[] columnParam = { "PK", "NAME", "ID" };

    @Parameters(name = "{index}:table1={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.normaltblTable(dbType));
    }

    public SelectComplexConditionTest(String tableName){
        BaseTestCase.normaltblTableName = tableName;
    }

    @Before
    public void prepareDate() throws Exception {
        normaltblPrepare(0, 50);
    }

    @Test
    public void conditionWithMutilCompareTest() throws Exception {
        long start1 = 7;
        long start2 = 4;
        long end1 = 49;
        long end2 = 18;
        float fl = 0.15f;
        String sql = "select * from " + normaltblTableName + " where pk>=? and pk>? and pk <=? and pk<?";
        List<Object> param = new ArrayList<Object>();
        param.add(start1);
        param.add(start2);
        param.add(end1);
        param.add(end2);
        String[] columnParam = { "PK", "NAME", "ID", "gmt_timestamp", "GMT_DATETIME", "FLOATCOL" };
        selectContentSameAssert(sql, columnParam, param);

        sql = "select * from " + normaltblTableName + " where pk>=? and pk>? and name like ? or gmt_datetime >?";
        param.clear();
        param.add(start1);
        param.add(start2);
        param.add(name);
        param.add(gmtNext);
        selectContentSameAssert(sql, columnParam, param);

        sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select * from " + normaltblTableName
              + " where pk>? or id<? and name like ? and gmt_datetime= ?";
        param.clear();
        param.add(start1);
        param.add(1500);
        param.add(name);
        param.add(gmt);
        selectContentSameAssert(sql, columnParam, param);

        sql = "select * from " + normaltblTableName
              + " where pk<=? and id>?  or name like ? and gmt_timestamp =? or floatCol=?";
        param.clear();
        param.add(start1);
        param.add(516);
        param.add(name);
        param.add(gmt);
        param.add(fl);
        selectConutAssert(sql, param);
        selectContentSameAssert(sql, columnParam, param);
    }

    /**
     * 业务模拟测试，测试的sql带and、between and和in以及group by,sum函数
     * 
     * @throws Exception
     */
    @Test
    public void sumBetweenAndInGroupTest() throws Exception {
        String sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select id, sum(pk) as p, sum(floatCol) as flo from "
                     + normaltblTableName
                     + " as a where floatCol >=? and gmt_timestamp between ? and ? and(name = ? and id in(?,?,?,?,?,?)) group by id";
        List<Object> param = new ArrayList<Object>();
        param.add(1.1);
        param.add(gmtBefore);
        param.add(gmtNext);
        param.add(name);
        param.add(0);
        param.add(100);
        param.add(200);
        param.add(300);
        param.add(400);
        param.add(1000);

        String[] columnParam = { "id", "p", "flo" };
        selectContentSameAssert(sql, columnParam, param);

        sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select id, sum(pk) as p, sum(floatCol) as flo ,name,gmt_timestamp,gmt_datetime  from "
              + normaltblTableName
              + " as a "
              + "where (( gmt_timestamp >= ? and gmt_timestamp<= ? and name = ? and id in(?,?,?,?,?,?)) and floatCol>=?) group by id";
        List<Object> param1 = new ArrayList<Object>();
        param1.add(gmtBefore);
        param1.add(gmtNext);
        param1.add(name);
        param1.add(0);
        param1.add(100);
        param1.add(200);
        param1.add(300);
        param1.add(400);
        param1.add(1000);
        param1.add(1.1);
        String[] columnParam1 = { "id", "p", "flo", "name", "gmt_timestamp", "gmt_datetime" };
        selectContentSameAssert(sql, columnParam1, param1);
    }

    /**
     * 测试的sql带and、between and和in以及group by
     * 
     * @throws Exception
     */
    @Test
    public void betweenAndInGroupTest() throws Exception {
        String sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select id as sid, gmt_timestamp  from "
                     + normaltblTableName + " as a where floatCol >=? and "
                     + "gmt_timestamp between ? and ? and(name = ? and id in(?,?,?,?,?,?)) group by id,gmt_timestamp";
        List<Object> param = new ArrayList<Object>();
        param.add(1.1);
        param.add(gmtBefore);
        param.add(gmtNext);
        param.add(name);
        param.add(0);
        param.add(100);
        param.add(200);
        param.add(300);
        param.add(400);
        param.add(1000);

        String[] columnParam = { "sid", "gmt_timestamp" };
        selectContentSameAssert(sql, columnParam, param);

        sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select id as sid, name,gmt_timestamp,gmt_datetime  from "
              + normaltblTableName
              + " as a "
              + "where (( gmt_timestamp >= ? and gmt_timestamp<= ? and name = ? and id in(?,?,?,?,?,?)) and floatCol>=?) group by id,gmt_timestamp";
        List<Object> param1 = new ArrayList<Object>();
        param1.add(gmtBefore);
        param1.add(gmtNext);
        param1.add(name);
        param1.add(0);
        param1.add(100);
        param1.add(200);
        param1.add(300);
        param1.add(400);
        param1.add(1000);
        param1.add(1.1);
        String[] columnParam1 = { "sid", "name", "gmt_timestamp", "gmt_datetime" };
        selectContentSameAssert(sql, columnParam1, param1);
    }

    /**
     * 查询语句后面带and 、between、group by
     * 
     * @throws Exception
     */
    @Test
    public void betweenGroupAndTest() throws Exception {
        String sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select name, gmt_timestamp from " + normaltblTableName
                     + " as a where floatCol>=? and gmt_timestamp  BETWEEN ? and ? " + "group by name, gmt_timestamp";
        List<Object> param = new ArrayList<Object>();
        param.add(1.1);
        param.add(gmtBefore);
        param.add(gmtNext);

        String[] columnParam = { "name", "gmt_timestamp" };
        selectContentSameAssert(sql, columnParam, param);

        sql = "select name, gmt_timestamp from " + normaltblTableName + " as a where floatCol>=? and "
              + "gmt_timestamp >=? and gmt_timestamp<=?  group by name, gmt_timestamp";
        selectContentSameAssert(sql, columnParam, param);
    }

    @Test
    public void betweenOrderAnd() throws Exception {
        String sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select gmt_timestamp, floatCol, id from "
                     + normaltblTableName + " as a where " + "id = ? and gmt_timestamp between ? and ? and "
                     + "(name = ? and gmt_timestamp = ?) order by gmt_timestamp";
        List<Object> param = new ArrayList<Object>();
        param.add(100);
        param.add(gmtBefore);
        param.add(gmtNext);
        param.add(name);
        param.add(gmt);

        String[] columnParam = { "gmt_timestamp", "floatCol", "id" };
        selectOrderAssertNotKeyCloumn(sql, columnParam, param, "gmt_timestamp");

        sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select gmt_timestamp, floatCol, id,name from "
              + normaltblTableName + " as a where "
              + "((gmt_timestamp >= ? and gmt_timestamp<= ? and name= ?  and id =? )"
              + " and floatCol>=? ) order by gmt_timestamp asc";
        List<Object> param1 = new ArrayList<Object>();
        param1.add(gmtBefore);
        param1.add(gmtNext);
        param1.add(name);
        param1.add(100);
        param1.add(1.1);
        String[] columnParam1 = { "gmt_timestamp", "floatCol", "id", "name" };
        selectOrderAssertNotKeyCloumn(sql, columnParam1, param1, "gmt_timestamp");
    }

    @Test
    public void groupByOrderBy() throws Exception {
        String sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ SELECT id,sum(pk) as sumPk,name from "
                     + normaltblTableName + " where id between" + " ? and ? GROUP BY id,name ORDER BY id";
        List<Object> param = new ArrayList<Object>();
        param.add(0);
        param.add(2000);
        String[] columnParam = { "id", "sumPk", "name" };
        selectContentSameAssert(sql, columnParam, param);

        sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ SELECT id,sum(pk) as sumPk,name from " + normaltblTableName
              + " where id between" + " ? and ? GROUP BY id,name ORDER BY id,name";
        selectContentSameAssert(sql, columnParam, param);

        sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ SELECT id,sum(pk) as sumPk,name from " + normaltblTableName
              + " where id between" + " ? and ? GROUP BY id,name ORDER BY name,id";
        selectContentSameAssert(sql, columnParam, param);
    }
}
