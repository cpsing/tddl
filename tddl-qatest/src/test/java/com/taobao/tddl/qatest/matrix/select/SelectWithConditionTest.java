package com.taobao.tddl.qatest.matrix.select;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.ExecuteTableName;
import com.taobao.tddl.qatest.util.EclipseParameterized;

/**
 * 带条件的选择查询
 * <p/>
 * Author By: yaolingling.pt Created Date: 2012-2-21 上午11:30:18
 */
@RunWith(EclipseParameterized.class)
public class SelectWithConditionTest extends BaseMatrixTestCase {

    @Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableName.normaltblStudentTable(dbType));
    }

    public SelectWithConditionTest(String normaltblTableName, String studentTableName){
        BaseTestCase.normaltblTableName = normaltblTableName;
        BaseTestCase.studentTableName = studentTableName;
    }

    @Before
    public void prepareDate() throws Exception {
        normaltblPrepare(0, 20);
        studentPrepare(0, MAX_DATA_SIZE);
    }

    @Test
    public void inTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk in (1,2,3)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void betweenTest() throws Exception {
        int start = 5;
        int end = 15;
        String sql = "select * from " + normaltblTableName + " where id between ? and ?";
        List<Object> param = new ArrayList<Object>();
        param.add(start);
        param.add(end);
        String[] columnParam = { "PK", "NAME", "ID" };
        selectContentSameAssert(sql, columnParam, param);
    }

    @Test
    public void constant() throws Exception {
        String sql = "select 1 a,2 from " + normaltblTableName;
        String[] columnParam = { "a", "2" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void selectFilterTest() throws Exception {
        String sql = "select id=id as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void selectFilterTest2() throws Exception {
        String sql = "select id=1 a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void selectFilterTest3() throws Exception {
        String sql = "select id and 1 a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void selectFilterTest4() throws Exception {
        String sql = "select 1=1 a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void selectFilterTest5() throws Exception {
        String sql = "select id in (1,2,3) a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void inWithParamTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk in (?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(Long.parseLong(1 + ""));
        param.add(Long.parseLong(2 + ""));
        param.add(Long.parseLong(3 + ""));
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, param);
    }

    @Test
    public void NotInTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk not in (?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(Long.parseLong(1 + ""));
        param.add(Long.parseLong(2 + ""));
        param.add(Long.parseLong(3 + ""));
        String[] columnParam = { "ID", "NAME" };
        selectContentSameAssert(sql, columnParam, param);
    }

    @Test
    public void isTrue() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select * from " + normaltblTableName + " where pk is true";
            String[] columnParam = { "ID", "NAME", "PK" };
            selectContentSameAssert(sql, columnParam, null);
            sql = "select id is true a,name from " + normaltblTableName;
            String[] columnParam1 = { "a", "NAME", };
            selectContentSameAssert(sql, columnParam1, null);
        }
    }

    @Test
    public void selectNot() throws Exception {
        String sql = "select not pk a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, null);
    }

    @Test
    public void selectNotFun() throws Exception {
        String[] columnParam = { "a" };
        String sql = "select not max(pk) a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, null);
    }

    @Test
    public void selectNotConstant() throws Exception {
        String sql = "select not (1+1) a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, null);
    }

    @Test
    public void selectNotWhereConstant() throws Exception {
        String sql = "select * from " + normaltblTableName + " where not (id=1)";
        String[] columnParam = { "ID", "NAME", "PK" };
        selectContentSameAssert(sql, columnParam, null);
    }

    @Test
    public void selectNotWhereConstantOr() throws Exception {
        String sql = "select * from " + normaltblTableName + " where not (id=1 or id=2)";
        String[] columnParam = { "ID", "NAME", "PK" };
        selectContentSameAssert(sql, columnParam, null);
    }

    @Test
    public void isNotTrue() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk is not true";
        String[] columnParam = { "ID", "NAME", "PK" };
        selectContentSameAssert(sql, columnParam, null);
    }

    @Test
    public void groupByWithCountTest() throws Exception {
        andorUpdateData("insert into " + normaltblTableName + " (pk,name) values(?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, newName }));
        mysqlUpdateData("insert into " + normaltblTableName + " (pk,name) values(?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, newName }));
        String sql = "select count(pk),name from " + normaltblTableName + " group by name";
        String[] columnParam = { "COUNT(PK)", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void groupByWithAscTest() throws Exception {
        andorUpdateData("insert into " + normaltblTableName + " (pk,name) values(?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, newName }));
        String sql = "select count(pk),name from " + normaltblTableName + " group by name asc";

        selectOrderAssert(sql, new String[] {}, Collections.EMPTY_LIST);
    }

    @Ignore("ob暂时不支持desc排序，而mysql会计算desc")
    @Test
    public void groupByWithDescTest() throws Exception {
        andorUpdateData("insert into " + normaltblTableName + " (pk,name) values(?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, newName }));
        String sql = "select count(pk),name from " + normaltblTableName + " group by name desc";
        selectOrderAssert(sql, new String[] {}, Collections.EMPTY_LIST);
    }

    @Test
    public void groupByWithMinMaxTest() throws Exception {
        andorUpdateData("insert into " + normaltblTableName + " (pk,name) values(?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, newName }));
        mysqlUpdateData("insert into " + normaltblTableName + " (pk,name) values(?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, newName }));
        String sql = "select name,min(pk) from " + normaltblTableName + " group by name";

        String[] columnParam = { "name", "min(pk)" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select name,max(pk) from " + normaltblTableName + " group by name";
        String[] param = { "name", "max(pk)" };
        selectOrderAssert(sql, param, Collections.EMPTY_LIST);

    }

    @Test
    public void groupByAvgTest() throws Exception {
        andorUpdateData("insert into " + normaltblTableName + " (pk,name) values(?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, newName }));
        mysqlUpdateData("insert into " + normaltblTableName + " (pk,name) values(?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, newName }));

        String sql = "select name,avg(pk) from " + normaltblTableName + " group by name";
        String[] columnParam = { "name", "avg(pk)" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void groupBySumTest() throws Exception {
        andorUpdateData("insert into " + normaltblTableName + " (pk,name) values(?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, newName }));
        mysqlUpdateData("insert into " + normaltblTableName + " (pk,name) values(?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, newName }));

        String sql = "select name,sum(pk) from " + normaltblTableName + " group by name";
        String[] columnParam = { "name", "sum(pk)" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void havingTest() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName
                     + " group by name having count(pk)>? order by name ";
        List<Object> param = new ArrayList<Object>();
        param.add(5L);
        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    /**
     * 查询字段没有带函数，having过滤带字段带函数，暂时只支持单机的
     */
    @Test
    public void havingFunTest() throws Exception {
        if (normaltblTableName.contains("oneGroup_oneAtom")) {
            String sql = "select name from " + normaltblTableName + " group by name having sum(pk) > ?";
            List<Object> param = new ArrayList<Object>();
            param.add(40l);
            String[] columnParam = { "name" };
            selectContentSameAssert(sql, columnParam, param);
        }
    }

    @Test
    public void orderByTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where name= ? order by pk";
        List<Object> param = new ArrayList<Object>();
        param.add(name);
        String[] columnParam = { "PK", "ID", "NAME" };
        selectOrderAssert(sql, columnParam, param);
    }

    @Test
    public void orderByAscTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where name= ? order by id asc";
        List<Object> param = new ArrayList<Object>();
        param.add(name);
        String[] columnParam = { "PK", "ID", "NAME" };
        selectOrderAssertNotKeyCloumn(sql, columnParam, param, "id");
    }

    @Test
    public void orderByDescTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where name= ? order by id desc";
        List<Object> param = new ArrayList<Object>();
        param.add(name);
        String[] columnParam = { "PK", "ID", "NAME" };
        selectOrderAssertNotKeyCloumn(sql, columnParam, param, "id");
    }

    @Test
    public void orderByNotAppointFieldTest() throws Exception {
        String sql = "select name,pk from " + normaltblTableName + " where name= ? order by id";
        List<Object> param = new ArrayList<Object>();
        param.add(name);
        String[] columnParam = { "name", "pk" };
        selectContentSameAssert(sql, columnParam, param);
    }

    /**
     * order by 后面跟多个排序字段
     */
    @Test
    public void orderByMutilValueTest() throws Exception {
        String sql = "select name,pk from " + normaltblTableName + " where name= ? order by gmt_create,id";
        List<Object> param = new ArrayList<Object>();
        param.add(name);
        String[] columnParam = { "name", "pk" };
        selectContentSameAssert(sql, columnParam, param);
    }

    @Test
    public void groupByOrderbyTest() throws Exception {
        andorUpdateData("insert into " + normaltblTableName + " (pk,name) values(?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, newName }));
        mysqlUpdateData("insert into " + normaltblTableName + " (pk,name) values(?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, newName }));

        String sql = "select name,count(a.pk) as c from " + normaltblTableName + " as a group by name order by name";
        String[] columnParam = { "name", "c" };
        selectOrderAssertNotKeyCloumn(sql, columnParam, Collections.EMPTY_LIST, "name");
    }

    @Test
    public void groupByOrderbyFunctionCloumTest() throws Exception {
        andorUpdateData("insert into " + normaltblTableName + " (pk,name) values(?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, newName }));
        mysqlUpdateData("insert into " + normaltblTableName + " (pk,name) values(?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, newName }));

        String sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */select name,count(pk) from " + normaltblTableName
                     + " group by name order by count(pk)";
        String[] columnParam = { "name", "count(pk)" };
        selectOrderAssertNotKeyCloumn(sql, columnParam, Collections.EMPTY_LIST, "count(pk)");
    }

    @Test
    public void AndTest() throws Exception {
        int i = 2;
        String sql = "select * from " + normaltblTableName + " where name= ? and id= ?";
        List<Object> param = new ArrayList<Object>();
        param.add(name);
        param.add(i);
        String[] columnParam = { "name", "pk" };
        selectContentSameAssert(sql, columnParam, param);
    }

    @Test
    public void orWithSameFiledTest() throws Exception {
        long pk = 2l;
        long opk = 3l;

        String sql = "select * from " + normaltblTableName + " where pk= ? or pk=?";
        List<Object> param = new ArrayList<Object>();
        param.add(pk);
        param.add(opk);
        String[] columnParam = { "name", "pk", "id" };
        selectContentSameAssert(sql, columnParam, param);
    }

    @Test
    public void distinctTest() throws Exception {
        String sql = "select distinct name from " + normaltblTableName;
        String[] columnParam = { "name" };
        selectContentSameAssert(sql, columnParam, null);
    }

    @Test
    public void distinctWithCountDistinctTest() throws Exception {
        {
            String sql = "select count(distinct id) from " + normaltblTableName + " as t1 where pk>1";
            String[] columnParam = { "count(distinct id)" };
            selectContentSameAssert(sql, columnParam, null);
        }

        {
            String sql = "select count(distinct id) from " + normaltblTableName + " where pk>1";
            String[] columnParam = { "count(distinct id)" };
            selectContentSameAssert(sql, columnParam, null);
        }

        {
            String sql = "select count(distinct id) k from " + normaltblTableName + " as t1 where pk>1";
            String[] columnParam = { "k" };
            selectContentSameAssert(sql, columnParam, null);
        }

    }

    @Test
    public void distinctOrderByTest() throws Exception {
        // order by和distinct不一致，只能用临时表
        String sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */select  distinct name from " + normaltblTableName
                     + " where name=? order by id";
        List<Object> param = new ArrayList<Object>();
        param.add(name);
        try {
            rc = andorQueryData(sql, param);
            Assert.assertEquals(1, resultsSize(rc));
        } finally {
            if (rc != null) {
                rc.close();
            }

        }
    }

    @Ignore("目前不支持distinctrow")
    @Test
    public void distinctrowTest() throws Exception {
        String sql = "select distinctrow name from " + normaltblTableName + " where name=?";
        List<Object> param = new ArrayList<Object>();
        param.add(name);
        try {
            rc = andorQueryData(sql, param);
            Assert.assertEquals(1, resultsSize(rc));
        } finally {
            rc.close();
        }
    }

    @Test
    public void orWithDifFiledTest() throws Exception {
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
    public void limitWithStart() throws Exception {
        int start = 5;
        int limit = 6;
        String sql = "SELECT * FROM " + normaltblTableName + " order by pk LIMIT " + start + "," + limit;
        String[] columnParam = { "name", "pk", "id" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void selectLimit() throws Exception {
        int start = 5;
        int limit = 1;
        String sql = "select * from " + studentTableName + " as nor1 ,(select pk from " + normaltblTableName
                     + " where name=? limit ?,?) as nor2 where nor1.id=nor2.pk";

        List<Object> param = new ArrayList<Object>();
        param.add(name);
        param.add(start);
        param.add(limit);
        selectConutAssert(sql, param);
    }

    @Test
    public void limitWithoutStart() throws Exception {
        int limit = 50;
        String sql = "SELECT * FROM " + normaltblTableName + " LIMIT " + limit;
        String[] columnParam = { "name", "pk", "id" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    // 狄龙项目的
    @Test
    public void groupByScalarFunction() throws Exception {
        if (!normaltblTableName.startsWith("ob")) { // ob不支持
            String sql = "SELECT  COUNT(1) daily_illegal,DATE_FORMAT(gmt_create, '%Y-%m-%d') d ,name FROM "
                         + normaltblTableName + " group by DATE_FORMAT(gmt_create, '%Y-%m-%d'),name";
            String[] columnParam = { "daily_illegal", "d", "name" };
            selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
        }
    }

    @Test
    public void groupByLimitTest() throws Exception {
        andorUpdateData("insert into " + normaltblTableName + " (pk,name) values(?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, newName }));
        mysqlUpdateData("insert into " + normaltblTableName + " (pk,name) values(?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, newName }));

        String sql = "select name,count(pk) from " + normaltblTableName + " group by name limit 1";
        String[] columnParam = { "name", "count(pk)" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void orderByLimitTest() throws Exception {
        andorUpdateData("insert into " + normaltblTableName + " (pk,name) values(?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, newName }));
        mysqlUpdateData("insert into " + normaltblTableName + " (pk,name) values(?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, newName }));

        String sql = "select * from " + normaltblTableName + " where name=? order by pk limit 10 ";
        List<Object> param = new ArrayList<Object>();
        param.add(name);
        String[] columnParam = { "name", "pk" };
        selectOrderAssert(sql, columnParam, param);

        sql = "select * from " + normaltblTableName + " where name=? order by pk desc limit 10 ";
        selectOrderAssert(sql, columnParam, param);
    }

    @Test
    public void dateTypeWithLimit() throws Exception {
        String sql = "select * from "
                     + normaltblTableName
                     + " where gmt_timestamp>? and gmt_timestamp <? and name like ? order by gmt_timestamp desc limit 2,5";
        List<Object> param = new ArrayList<Object>();
        param.add(gmtBefore);
        param.add(gmtNext);
        param.add(name);
        selectConutAssert(sql, param);

        sql = "select * from " + normaltblTableName
              + " where gmt_timestamp>=? and gmt_timestamp <=? and name like ? order by gmt_timestamp desc limit 2,5";
        param.clear();
        param.add(gmtBefore);
        param.add(gmtNext);
        param.add(name);
        selectConutAssert(sql, param);

        sql = "select * from " + normaltblTableName
              + " where gmt_timestamp>? and gmt_timestamp <? and name like ? order by gmt_timestamp desc limit 10,5";

        param.clear();
        param.add(gmtBefore);
        param.add(gmtNext);
        param.add(name);
        selectConutAssert(sql, param);
    }

    @Test
    public void timestampTypeWithLimit() throws Exception {
        String sql = "select * from "
                     + normaltblTableName
                     + " where gmt_timestamp>? and gmt_timestamp <? and name like ? order by gmt_timestamp desc limit 2,5";
        List<Object> param = new ArrayList<Object>();
        param.add(gmtBefore);
        param.add(gmtNext);
        param.add(name);
        selectConutAssert(sql, param);

        sql = "select * from " + normaltblTableName
              + " where gmt_timestamp>=? and gmt_timestamp <=? and name like ? order by gmt_timestamp desc limit 2,5";
        param.clear();
        param.add(gmtBefore);
        param.add(gmtNext);
        param.add(name);
        selectConutAssert(sql, param);

        sql = "select * from " + normaltblTableName
              + " where gmt_timestamp>? and gmt_timestamp <? and name like ? order by gmt_timestamp desc limit 10,5";
        param.clear();
        param.add(gmtBefore);
        param.add(gmtNext);
        param.add(name);
        selectConutAssert(sql, param);
    }

    @Test
    public void datetimeTypeWithLimit() throws Exception {
        String sql = "select * from " + normaltblTableName
                     + " where gmt_datetime>? and gmt_datetime <? and name like ? order by gmt_datetime desc limit 2,5";
        List<Object> param = new ArrayList<Object>();
        param.add(gmtBefore);
        param.add(gmtNext);
        param.add(name);
        selectConutAssert(sql, param);

        sql = "select * from " + normaltblTableName
              + " where gmt_datetime>=? and gmt_datetime <=? and name like ? order by gmt_datetime desc limit 2,5";
        param.clear();
        param.add(gmtBefore);
        param.add(gmtNext);
        param.add(name);
        selectConutAssert(sql, param);

        sql = "select * from " + normaltblTableName
              + " where gmt_datetime>? and gmt_datetime <? and name like ? order by gmt_datetime desc limit 10,5";
        param.clear();
        param.add(gmtBefore);
        param.add(gmtNext);
        param.add(name);
        selectConutAssert(sql, param);
    }

    @Test
    public void limitError() throws Exception {
        int limit = -3;
        List<Object> param = new ArrayList<Object>();
        param.add(limit);
        String sql = "SELECT * FROM " + normaltblTableName + " LIMIT ";
        try {
            rc = andorQueryData(sql, param);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertNotNull(e.getMessage());
        }
    }

    @Ignore("目前不支持union")
    @Test
    public void unionTest() throws Exception {
    }

}
