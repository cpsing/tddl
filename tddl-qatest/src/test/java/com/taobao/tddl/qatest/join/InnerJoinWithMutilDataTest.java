package com.taobao.tddl.qatest.join;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseTddlTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.util.EclipseParameterized;
import com.taobao.tddl.qatest.util.ExecuteTableName;

/**
 * Comment for InnerJoinTest
 * <p/>
 * Author By: zhuoxue.yll Created Date: 2012-3-13 上午09:42:46
 */
@RunWith(EclipseParameterized.class)
public class InnerJoinWithMutilDataTest extends BaseTddlTestCase {

    String[] columnParam = { "host_id", "host_name", "hostgroup_id", "hostgroup_name" };

    @Parameters(name = "{index}:table0={0},table1={1},table2={2},table3={3},table4={4}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableName.hostinfoHostgoupStudentModuleinfoModulehostTable(dbType));
    }

    public InnerJoinWithMutilDataTest(String monitor_host_infoTableName, String monitor_hostgroup_infoTableName,
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
    public void innerJoinTest() throws Exception {
        String sql = "select " + host_info + ".host_id," + host_info + ".host_name," + host_info + ".hostgroup_id,"
                     + hostgroup_info + ".hostgroup_name " + " from " + host_info + " inner join " + hostgroup_info
                     + "  " + " on " + host_info + ".hostgroup_id=" + hostgroup_info + ".hostgroup_id";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void InnerJoinWithSomeValueTest() throws Exception {
        // bdb数据库join根据desc排序抛出空指针异常,暂时忽略
        if (host_info.contains("mysql")) {
            String sql = "select " + host_info + ".host_id," + host_info + ".host_name," + host_info + ".hostgroup_id,"
                         + hostgroup_info + ".hostgroup_name " + " from " + hostgroup_info + " inner join " + host_info
                         + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup_info + ".hostgroup_id";
            selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
        }
    }

    @Test
    public void InnerJoinWithAndTest() throws Exception {
        String sql = "select " + host_info + ".host_id," + "" + host_info + ".host_name," + host_info
                     + ".hostgroup_id," + hostgroup_info + ".hostgroup_name " + "from " + hostgroup_info
                     + " inner join " + host_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup_info
                     + ".hostgroup_id where " + host_info + ".host_name='hostname52'";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 对join的缓存测试
     * 
     * @throws Exception
     */
    @Test
    public void joinCacheTest() throws Exception {
        for (int i = 50; i < 60; i++) {
            String sql = "select " + host_info + ".host_id," + "" + host_info + ".host_name," + host_info
                         + ".hostgroup_id," + hostgroup_info + ".hostgroup_name " + "from " + hostgroup_info
                         + " inner join " + host_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup_info
                         + ".hostgroup_id where " + host_info + ".host_name='hostname" + i + "'";
            selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
        }
    }

    @Test
    public void InnerJoinWithWhereNumFiledTest() throws Exception {
        String sql = "select " + host_info + ".host_id," + "" + host_info + ".host_name," + host_info
                     + ".hostgroup_id," + hostgroup_info + ".hostgroup_name " + "from " + hostgroup_info
                     + " inner join " + host_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup_info
                     + ".hostgroup_id where " + host_info + ".host_id=52";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select " + host_info + ".host_id," + "" + host_info + ".host_name," + host_info + ".hostgroup_id,"
              + hostgroup_info + ".hostgroup_name " + "from " + hostgroup_info + " inner join " + host_info + "  "
              + "on " + host_info + ".hostgroup_id=" + hostgroup_info + ".hostgroup_id where " + host_info
              + ".host_id>80";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void InnerJoinWithWhereStringFieldTest() throws Exception {
        String sql = "select " + host_info + ".host_id," + "" + host_info + ".host_name," + host_info
                     + ".hostgroup_id," + hostgroup_info + ".hostgroup_name " + "from " + hostgroup_info
                     + " inner join " + host_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup_info
                     + ".hostgroup_id" + " where " + host_info + ".host_name='hostname90'";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void InnerJoinWithWhereAndTest() throws Exception {
        String sql = "select " + host_info + ".host_id," + "" + host_info + ".host_name," + host_info
                     + ".hostgroup_id," + hostgroup_info + ".hostgroup_name " + "from " + hostgroup_info
                     + " inner join " + host_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup_info
                     + ".hostgroup_id where " + host_info + ".host_name='hostname80' and " + hostgroup_info
                     + ".hostgroup_name='hostgroupname80'";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void InnerJoinWithWhereAndSameFiledTest() throws Exception {
        String sql = "select " + host_info + ".host_id," + "" + host_info + ".host_name," + host_info
                     + ".hostgroup_id," + hostgroup_info + ".hostgroup_name " + "from " + hostgroup_info
                     + " inner join " + host_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup_info
                     + ".hostgroup_id where " + hostgroup_info + ".hostgroup_id>20 and " + hostgroup_info
                     + ".hostgroup_id<80";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void InnerJoinWithWhereAndNoDataTest() throws Exception {
        String sql = "select " + host_info + ".host_id," + "" + host_info + ".host_name," + host_info
                     + ".hostgroup_id," + hostgroup_info + ".hostgroup_name " + "from " + hostgroup_info
                     + " inner join " + host_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup_info
                     + ".hostgroup_id where " + hostgroup_info + ".hostgroup_id<20 and " + hostgroup_info
                     + ".hostgroup_id>150";

        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void InnerJoinWithWhereOrTest() throws Exception {
        // bdb数据库join测试以下测试用例抛出异常，原因目前只支持单值查询
        if (host_info.contains("mysql")) {
            String sql = "select " + host_info + ".host_id," + "" + host_info + ".host_name," + host_info
                         + ".hostgroup_id," + hostgroup_info + ".hostgroup_name " + "from " + hostgroup_info
                         + " inner join " + host_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup_info
                         + ".hostgroup_id where " + host_info + ".host_name='hostname50' or " + hostgroup_info
                         + ".hostgroup_name='hostgroupname51'";
            selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
        }
    }

    @Test
    public void InnerJoinWithWhereBetweenTest() throws Exception {
        String sql = "select " + host_info + ".host_id," + "" + host_info + ".host_name," + host_info
                     + ".hostgroup_id," + hostgroup_info + ".hostgroup_name " + "from " + hostgroup_info
                     + " inner join " + host_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup_info
                     + ".hostgroup_id where " + host_info + ".hostgroup_id between 40 and 70";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void InnerJoinWithWhereLimitTest() throws Exception {
        String sql = "select " + host_info + ".host_id," + "" + host_info + ".host_name," + host_info
                     + ".hostgroup_id," + hostgroup_info + ".hostgroup_name " + "from " + hostgroup_info
                     + " inner join " + host_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup_info
                     + ".hostgroup_id where " + host_info + ".hostgroup_id between 40 and 70 limit 10";
        selectConutAssert(sql, Collections.EMPTY_LIST);
    }

    @Test
    public void InnerJoinWithWhereOrderByLimitTest() throws Exception {
        // join时不开启调整顺序，如果右表存在orderby，则会需要临时表
        String sql = "select " + host_info + ".host_id," + "" + host_info + ".host_name," + host_info
                     + ".hostgroup_id," + hostgroup_info + ".hostgroup_name " + "from " + host_info + " inner join "
                     + hostgroup_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup_info
                     + ".hostgroup_id where " + host_info + ".hostgroup_id between 40 and 70 order by " + host_info
                     + ".host_id limit 10";
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void InnerJoinThreeTableTest() throws Exception {
        String sql = "SELECT * FROM " + host_info + " inner JOIN " + hostgroup_info + " ON " + host_info
                     + ".hostgroup_id=" + hostgroup_info + ".hostgroup_id " + "inner JOIN " + studentTableName + " ON "
                     + hostgroup_info + ".hostgroup_id=" + studentTableName + ".id";
        String[] columnParam = { "host_id", "host_name", "hostgroup_id", "hostgroup_name", "name" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 收藏夹主要流程模拟测试 查询指定用户所收藏的商品信息 或者指定商品收藏用户信息
     */
    @Test
    public void InnerJoinThreeTableWithWhere() throws Exception {
        String sql = "SELECT * from " + host_info + " INNER JOIN " + module_host + " ON " + host_info + ".host_id="
                     + module_host + ".host_id INNER JOIN " + module_info + "  ON " + module_info + ".module_id="
                     + module_host + ".module_id where " + module_info + ".module_name='module1'";
        String[] columnParam = { "host_id", "host_name", "module_id", "id", "module_name" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void InnerJoinThreeTableWithWhereWithOr() throws Exception {
        String sql = "SELECT * from " + host_info + " INNER JOIN " + module_host + " ON " + host_info + ".host_id="
                     + module_host + ".host_id INNER JOIN " + module_info + "  ON " + module_info + ".module_id="
                     + module_host + ".module_id where " + module_info + ".module_name='module1' or " + module_info
                     + ".module_name='module4'";
        String[] columnParam = { "host_id", "host_name", "module_id", "id", "module_name" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void InnerJoinThreeTableWithWhereWithAnd() throws Exception {
        String sql = "SELECT * from " + host_info + " INNER JOIN " + module_host + " ON " + host_info + ".host_id="
                     + module_host + ".host_id INNER JOIN " + module_info + "  ON " + module_info + ".module_id="
                     + module_host + ".module_id where " + module_info + ".module_name='module1' and " + host_info
                     + ".host_name='hostname1'";
        String[] columnParam = { "host_id", "host_name", "module_id", "id", "module_name" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void InnerJoinWithAliasTest() throws Exception {
        String sql = "select a.host_id,a.host_name,a.hostgroup_id,b.hostgroup_name from " + host_info
                     + " a inner join " + hostgroup_info + " b " + "on a.hostgroup_id=b.hostgroup_id";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void InnerJoinWithAliasAsTest() throws Exception {
        String sql = "select a.host_id,a.host_name,a.hostgroup_id,b.hostgroup_name from " + host_info
                     + " as a inner join " + hostgroup_info + " as b " + "on a.hostgroup_id=b.hostgroup_id";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void InnerJoinWithOrderByTest() throws Exception {
        String sql = "select a.host_id,a.host_name,a.hostgroup_id,b.hostgroup_name from " + host_info
                     + " as a inner join " + hostgroup_info + " as b "
                     + "on a.hostgroup_id=b.hostgroup_id order by a.host_id asc";
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void InnerJoinWithOrderByascTest() throws Exception {
        String sql = "select a.host_id,a.host_name,a.hostgroup_id,b.hostgroup_name from " + host_info
                     + " as a inner join " + hostgroup_info + " as b "
                     + "on a.hostgroup_id=b.hostgroup_id order by a.host_id asc";
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void InnerJoinWithOrderBydescTest() throws Exception {
        // bdb数据库join根据desc排序抛出空指针异常,暂时忽略
        if (host_info.contains("mysql")) {
            String sql = "select a.host_id,a.host_name,a.hostgroup_id,b.hostgroup_name from " + host_info
                         + " as a inner join " + hostgroup_info + " as b "
                         + "on a.hostgroup_id=b.hostgroup_id order by a.host_id desc";
            selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
        }
    }

    @Test()
    public void InnerJoinWithSubQueryTest() throws Exception {
        String sql = "select t1.sum1,t2.count2 from (select sum(host_id) as sum1,station_id from " + host_info
                     + " group by station_id) t1 " + "join (select count(hostgroup_id) as count2,station_id from "
                     + hostgroup_info + " group by station_id) t2 on t1.station_id=t2.station_id";
        String[] columnParam = { "sum1", "count2" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

}
