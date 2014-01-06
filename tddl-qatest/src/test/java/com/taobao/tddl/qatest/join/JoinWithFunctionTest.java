package com.taobao.tddl.qatest.join;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseAndorTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.util.EclipseParameterized;
import com.taobao.tddl.qatest.util.ExecuteTableName;

/**
 * Comment for InnerJoinTest
 * <p/>
 * Author By: zhuoxue.yll Created Date: 2012-3-13 上午09:42:46
 */
@RunWith(EclipseParameterized.class)
public class JoinWithFunctionTest extends BaseAndorTestCase {

    @Parameters(name = "{index}:table0={0},table1={1},table2={2},table3={3},table4={4}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableName.hostinfoHostgoupStudentModuleinfoModulehostTable(dbType));
    }

    public JoinWithFunctionTest(String monitor_host_infoTableName, String monitor_hostgroup_infoTableName,
                                String studentTableName, String monitor_module_infoTableName,
                                String monitor_module_hostTableName) throws Exception{
        BaseTestCase.host_info = monitor_host_infoTableName;
        BaseTestCase.hostgroup_info = monitor_hostgroup_infoTableName;
        BaseTestCase.studentTableName = studentTableName;
        BaseTestCase.module_info = monitor_module_infoTableName;
        BaseTestCase.module_host = monitor_module_hostTableName;
        initData();
    }

    public void initData() throws Exception {
        con = getConnection();
        andorCon = us.getConnection();
        hostinfoPrepare(0, 100);
        hostgroupInfoPrepare(50, 200);
        module_infoPrepare(0, 40);
        module_hostPrepare(1, 80);
        studentPrepare(65, 80);
    }

    @After
    public void destory() throws Exception {
        psConRcRsClose(rc, rs);
    }

    @Test
    public void joinMaxMinTest() throws Exception {
        String[] columnParamMax = { "max(" + host_info + ".host_id)" };
        String sql = "select max(" + host_info + ".host_id)," + host_info + ".host_name," + host_info
                     + ".hostgroup_id," + hostgroup_info + ".hostgroup_name " + " from " + host_info + " inner join "
                     + hostgroup_info + "  " + " on " + host_info + ".hostgroup_id=" + hostgroup_info + ".hostgroup_id";
        selectContentSameAssert(sql, columnParamMax, Collections.EMPTY_LIST);

        String[] columnParamMin = { "min(" + host_info + ".host_id)" };
        sql = "select min(" + host_info + ".host_id)," + host_info + ".host_name," + host_info + ".hostgroup_id,"
              + hostgroup_info + ".hostgroup_name " + " from " + host_info + " inner join " + hostgroup_info + "  "
              + " on " + host_info + ".hostgroup_id=" + hostgroup_info + ".hostgroup_id";
        selectContentSameAssert(sql, columnParamMin, Collections.EMPTY_LIST);

        String[] columnParamAlias = { "min" };
        sql = "select min(" + host_info + ".host_id) min," + host_info + ".host_name," + host_info + ".hostgroup_id,"
              + hostgroup_info + ".hostgroup_name " + " from " + host_info + " inner join " + hostgroup_info + "  "
              + " on " + host_info + ".hostgroup_id=" + hostgroup_info + ".hostgroup_id";
        selectContentSameAssert(sql, columnParamAlias, Collections.EMPTY_LIST);
    }

    @Test
    public void joinSumTest() throws Exception {
        String[] columnParamSum = { "sum(" + host_info + ".host_id)" };
        String sql = "select sum(" + host_info + ".host_id)," + host_info + ".host_name," + host_info
                     + ".hostgroup_id," + hostgroup_info + ".hostgroup_name " + " from " + host_info + " inner join "
                     + hostgroup_info + "  " + " on " + host_info + ".hostgroup_id=" + hostgroup_info + ".hostgroup_id";
        selectContentSameAssert(sql, columnParamSum, Collections.EMPTY_LIST);
    }

    @Test
    public void joinAvgTest() throws Exception {
        String[] columnParamAvg = { "avg(" + host_info + ".host_id)" };
        String sql = "select avg(" + host_info + ".host_id)," + host_info + ".host_name," + host_info
                     + ".hostgroup_id," + hostgroup_info + ".hostgroup_name " + " from " + host_info + " inner join "
                     + hostgroup_info + "  " + " on " + host_info + ".hostgroup_id=" + hostgroup_info + ".hostgroup_id";
        selectContentSameAssert(sql, columnParamAvg, Collections.EMPTY_LIST);
    }

    @Test
    public void joinCountTest() throws Exception {
        String[] columnParamCount = { "count(" + host_info + ".host_id)" };
        String sql = "select count(" + host_info + ".host_id)," + host_info + ".host_name," + host_info
                     + ".hostgroup_id," + hostgroup_info + ".hostgroup_name " + " from " + host_info + " inner join "
                     + hostgroup_info + "  " + " on " + host_info + ".hostgroup_id=" + hostgroup_info + ".hostgroup_id";
        selectContentSameAssert(sql, columnParamCount, Collections.EMPTY_LIST);

        String[] columnCount = { "count(*)" };
        sql = "select count(*)," + host_info + ".host_name," + host_info + ".hostgroup_id," + hostgroup_info
              + ".hostgroup_name " + " from " + host_info + " inner join " + hostgroup_info + "  " + " on " + host_info
              + ".hostgroup_id=" + hostgroup_info + ".hostgroup_id";
        selectContentSameAssert(sql, columnCount, Collections.EMPTY_LIST);
    }

    @Test
    public void JoinWithAndMaxTest() throws Exception {
        String[] columnParam = { "max(" + host_info + ".host_id)" };
        String sql = "select max(" + host_info + ".host_id)," + "" + host_info + ".host_name," + host_info
                     + ".hostgroup_id," + hostgroup_info + ".hostgroup_name " + "from " + hostgroup_info
                     + " inner join " + host_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup_info
                     + ".hostgroup_id where " + host_info + ".host_name='hostname52'";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void JoinWithWhereAndSumTest() throws Exception {
        String[] columnParam = { "sum(" + host_info + ".host_id)" };
        String sql = "select sum(" + host_info + ".host_id)," + "" + host_info + ".host_name," + host_info
                     + ".hostgroup_id," + hostgroup_info + ".hostgroup_name " + "from " + hostgroup_info
                     + " inner join " + host_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup_info
                     + ".hostgroup_id where " + host_info + ".host_name='hostname80' and " + hostgroup_info
                     + ".hostgroup_name='hostgroupname80'";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void JoinWithWhereOrCountTest() throws Exception {
        String[] columnParam = { "count(" + host_info + ".host_id)" };
        // bdb数据库join测试以下测试用例抛出异常，原因目前只支持单值查询
        if (host_info.contains("mysql")) {
            String sql = "select count(" + host_info + ".host_id)," + "" + host_info + ".host_name," + host_info
                         + ".hostgroup_id," + hostgroup_info + ".hostgroup_name " + "from " + hostgroup_info
                         + " inner join " + host_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup_info
                         + ".hostgroup_id where " + host_info + ".host_name='hostname50' or " + hostgroup_info
                         + ".hostgroup_name='hostgroupname51'";
            selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
        }
    }

    @Test
    public void JoinWithWhereBetweenSumTest() throws Exception {
        String[] columnParam = { "sum(" + host_info + ".host_id)" };
        String sql = "select sum(" + host_info + ".host_id)," + "" + host_info + ".host_name," + host_info
                     + ".hostgroup_id," + hostgroup_info + ".hostgroup_name " + "from " + hostgroup_info
                     + " inner join " + host_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup_info
                     + ".hostgroup_id where " + host_info + ".hostgroup_id between 40 and 70";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void JoinWithWhereLimitCountTest() throws Exception {
        String sql = "select count(" + host_info + ".host_id)," + "" + host_info + ".host_name," + host_info
                     + ".hostgroup_id," + hostgroup_info + ".hostgroup_name " + "from " + hostgroup_info
                     + " inner join " + host_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup_info
                     + ".hostgroup_id where " + host_info + ".hostgroup_id between 40 and 70 limit 10";

        selectConutAssert(sql, Collections.EMPTY_LIST);
    }

    @Test
    public void JoinWithWhereOrderByLimitMinTest() throws Exception {
        String[] columnParam = { "min(" + host_info + ".host_id)" };
        String sql = "select min(" + host_info + ".host_id)," + "" + host_info + ".host_name," + host_info
                     + ".hostgroup_id," + hostgroup_info + ".hostgroup_name " + "from " + host_info + " inner join "
                     + hostgroup_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup_info
                     + ".hostgroup_id where " + host_info + ".hostgroup_id order by " + host_info
                     + ".hostgroup_id limit 10";
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void InnerJoinWithGroupbyTest() throws Exception {
        String[] columnParam = { "min(" + host_info + ".host_id)" };
        String sql = "select min(" + host_info + ".host_id)," + "" + host_info + ".host_name name from " + host_info
                     + " inner join " + hostgroup_info + " on " + host_info + ".hostgroup_id=" + hostgroup_info
                     + ".hostgroup_id group by name";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void InnerJoinWithOrderGroupbyTest() throws Exception {
        String[] columnParam = { "min(" + host_info + ".host_id)" };
        String sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */ select min(" + host_info + ".host_id)," + "" + host_info
                     + ".host_name name from " + hostgroup_info + " inner join " + host_info + " on " + host_info
                     + ".hostgroup_id=" + hostgroup_info + ".hostgroup_id group by name order by " + host_info
                     + ".hostgroup_id";
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void JoinThreeTableWithWhereCount() throws Exception {
        String[] columnParam = { "count(*)" };
        String sql = "SELECT count(*) from " + host_info + " INNER JOIN " + module_host + " ON " + host_info
                     + ".host_id=" + module_host + ".host_id INNER JOIN " + module_info + "  ON " + module_info
                     + ".module_id=" + module_host + ".module_id where " + module_info + ".module_name='module1' or "
                     + module_info + ".module_name='module4'";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void JoinWithAliasSumTest() throws Exception {
        String[] columnParam = { "sum(a.host_id)" };
        String sql = "select sum(a.host_id),a.host_name,a.hostgroup_id,b.hostgroup_name from " + host_info
                     + " a inner join " + hostgroup_info + " b " + "on a.hostgroup_id=b.hostgroup_id";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

}
