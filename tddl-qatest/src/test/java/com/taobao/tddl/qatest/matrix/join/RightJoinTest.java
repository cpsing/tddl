package com.taobao.tddl.qatest.matrix.join;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.util.EclipseParameterized;
import com.taobao.tddl.qatest.util.ExecuteTableName;

/**
 * LeftJoin测试，bdb不支持LeftJoin，只有当dbType="mysql"时测试用例才会运行
 * <p/>
 * Author By: zhuoxue.yll Created Date: 2012-3-13 上午09:42:46
 */
@RunWith(EclipseParameterized.class)
public class RightJoinTest extends BaseMatrixTestCase {

    String[] columnParam = { "host_id", "host_name", "hostgroup_id", "hostgroup_name" };

    @Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableName.mysqlHostinfoHostgroupTable(dbType));
    }

    public RightJoinTest(String monitor_host_infoTableName, String monitor_hostgroup_infoTableName) throws Exception{
        BaseTestCase.host_info = monitor_host_infoTableName;
        BaseTestCase.hostgroup = monitor_hostgroup_infoTableName;
        initData();
    }

    public void initData() throws Exception {
        con = getConnection();
        andorCon = us.getConnection();

        hostgroupPrepare(5, 20);
        hostinfoPrepare(0, 10);
        hostgroupDataAdd(20, 30, 8l);
    }

    @After
    public void destory() throws Exception {
        psConRcRsClose(rc, rs);
    }

    @Test
    public void rightJoinTest() throws Exception {
        String sql = "select " + host_info + ".host_id," + host_info + ".host_name," + host_info + ".hostgroup_id,"
                     + hostgroup + ".hostgroup_name from " + host_info + " right join " + hostgroup + "  " + "on "
                     + hostgroup + ".module_id=" + host_info + ".hostgroup_id";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void rightJoinWithWhercest() throws Exception {
        String sql = "select " + host_info + ".host_id," + host_info + ".host_name," + host_info + ".hostgroup_id,"
                     + hostgroup + ".hostgroup_name from " + host_info + " right join " + hostgroup + "  " + "on "
                     + hostgroup + ".module_id=" + host_info + ".hostgroup_id where " + hostgroup
                     + ".hostgroup_name='hostgroupname0'";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void rightJoinWithAndTest() throws Exception {
        String sql = "select " + host_info + ".host_id," + host_info + ".host_name," + host_info + ".hostgroup_id,"
                     + hostgroup + ".hostgroup_name from " + host_info + " right join " + hostgroup + "  " + "on "
                     + hostgroup + ".module_id=" + host_info + ".hostgroup_id where " + hostgroup
                     + ".hostgroup_name='hostgroupname0'" + " and " + host_info + ".host_name='hostname0'";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void rightJoinWithOrTest() throws Exception {
        String sql = "select " + host_info + ".host_id," + host_info + ".host_name," + host_info + ".hostgroup_id,"
                     + hostgroup + ".hostgroup_name from " + host_info + " right join " + hostgroup + "  " + "on "
                     + hostgroup + ".module_id=" + host_info + ".hostgroup_id where " + hostgroup
                     + ".hostgroup_name='hostgroupname0'" + " OR " + hostgroup + ".hostgroup_name='hostgroupname1'";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void rightJoinWithLimitTest() throws Exception {
        String sql = "select " + host_info + ".host_id," + host_info + ".host_name," + host_info + ".hostgroup_id,"
                     + hostgroup + ".hostgroup_name from " + host_info + " right join " + hostgroup + "  " + "on "
                     + hostgroup + ".module_id=" + host_info + ".hostgroup_id where " + hostgroup
                     + ".hostgroup_name='hostgroupname0'" + " limit 1";
        selectConutAssert(sql, Collections.EMPTY_LIST);
    }

    @Test
    public void rightJoinWithAliasTest() throws Exception {
        String sql = "select a.host_id,a.host_name,a.hostgroup_id,b.hostgroup_name from  " + host_info
                     + " a right join  " + hostgroup + " b " + "on b.module_id=a.hostgroup_id";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void rightJoinWithAliasAsTest() throws Exception {
        String sql = "select a.host_id,a.host_name,a.hostgroup_id,b.hostgroup_name from " + host_info
                     + " as a right join  " + hostgroup + " as b " + "on b.module_id=a.hostgroup_id";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void rightJoinWithOrderByTest() throws Exception {
        String sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */select a.host_id,a.host_name,a.hostgroup_id,b.hostgroup_name from "
                     + host_info
                     + " as a right join "
                     + hostgroup
                     + " as b "
                     + "on b.hostgroup_id=a.hostgroup_id where b.hostgroup_name='hostgroupname0' order by a.host_id";
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
        sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */select a.host_id,a.host_name,a.hostgroup_id,b.hostgroup_name from "
              + host_info
              + " as a right join "
              + hostgroup
              + " as b "
              + "on b.hostgroup_id=a.hostgroup_id where b.hostgroup_name='hostgroupname0' order by a.host_id asc";
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
        sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */select a.host_id,a.host_name,a.hostgroup_id,b.hostgroup_name from "
              + host_info
              + " as a right join "
              + hostgroup
              + " as b "
              + "on b.hostgroup_id=a.hostgroup_id where b.hostgroup_name='hostgroupname0' order by a.host_id desc";
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void rightJoinWithSubQueryTest() throws Exception {
        String sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */SELECT sumId ,host_name , a.hostgroup_id as aid ,b.hostgroup_id  as bid from "
                     + "( select  sum(host_id) as sumId,host_name,hostgroup_id from "
                     + host_info
                     + " where host_id BETWEEN ? and ? GROUP BY host_name ORDER BY hostgroup_id LIMIT ?) as a "
                     + "RIGHT JOIN (SELECT SUM(hostgroup_id) , hostgroup_name,hostgroup_id  from "
                     + hostgroup
                     + " where hostgroup_id "
                     + "BETWEEN ? and ? GROUP BY hostgroup_name ) as b ON a.hostgroup_id=b.hostgroup_id ORDER BY sumId DESC";
        List<Object> param = new ArrayList<Object>();
        param.add(0);
        param.add(100);
        param.add(10);
        param.add(1);
        param.add(20);
        String[] columnParam = { "sumId", "host_name", "aid", "bid" };
        selectContentSameAssert(sql, columnParam, param);
    }
}
