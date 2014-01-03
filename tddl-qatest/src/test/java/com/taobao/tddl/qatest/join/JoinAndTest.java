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
 * Comment for JoinAndTest
 * <p/>
 * Author By: zhuoxue.yll Created Date: 2012-12-4 上午10:57:08
 */
@RunWith(EclipseParameterized.class)
public class JoinAndTest extends BaseAndorTestCase {

    String[] columnParam = { "host_id", "host_name", "hostgroup_id", "hostgroup_name" };
    String[] joinType    = { "inner", "left", "right" };
    String[] innerJoin   = { "inner" };
    String[] leftJoin    = { "left" };
    String   sql         = null;
    String   hint        = "";

    @Parameters(name = "{index}:table0={0},table1={1},table2={2},table3={3},table4={4}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableName.hostinfoHostgoupStudentModuleinfoModulehostTable(dbType));
    }

    public JoinAndTest(String monitor_host_infoTableName, String monitor_hostgroup_infoTableName,
                       String studentTableName, String monitor_module_infoTableName, String monitor_module_hostTableName)
                                                                                                                         throws Exception{
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
    public void AndTest() throws Exception {
        for (int i = 0; i < joinType.length; i++) {
            sql = "select " + host_info + ".host_id," + "" + host_info + ".host_name," + host_info + ".hostgroup_id,"
                  + hostgroup_info + ".hostgroup_name " + "from " + hostgroup_info + " " + joinType[i] + " join "
                  + host_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup_info + ".hostgroup_id and "
                  + host_info + ".host_id=52 where " + hostgroup_info + ".hostgroup_id=52";
            if (!host_info.contains("oneGroup_oneAtom")) sql = hint + sql;
            selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
        }
    }

    @Test
    public void AndNumFiledTest() throws Exception {
        for (int i = 0; i < innerJoin.length; i++) {
            sql = "select " + host_info + ".host_id," + "" + host_info + ".host_name," + host_info + ".hostgroup_id,"
                  + hostgroup_info + ".hostgroup_name " + "from " + hostgroup_info + " " + joinType[i] + " join "
                  + host_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup_info + ".hostgroup_id where "
                  + host_info + ".host_id=52";
            // if (!host_info.contains("oneGroup_oneAtom"))
            // sql = hint+sql;
            selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

            sql = "select " + host_info + ".host_id," + "" + host_info + ".host_name," + host_info + ".hostgroup_id,"
                  + hostgroup_info + ".hostgroup_name " + "from " + hostgroup_info + " " + joinType[i] + " join "
                  + host_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup_info + ".hostgroup_id where "
                  + host_info + ".host_id>80";
            // if (!host_info.contains("oneGroup_oneAtom"))
            // sql = hint+sql;
            selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
        }
    }

    @Test
    public void StringFieldTest() throws Exception {
        for (int i = 0; i < innerJoin.length; i++) {
            sql = "select " + host_info + ".host_id," + "" + host_info + ".host_name," + host_info + ".hostgroup_id,"
                  + hostgroup_info + ".hostgroup_name " + "from " + hostgroup_info + " " + joinType[i] + " join "
                  + host_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup_info + ".hostgroup_id"
                  + " where " + host_info + ".host_name='hostname90'";
            if (!host_info.contains("oneGroup_oneAtom")) sql = hint + sql;
            selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
        }
    }

    @Test
    public void ThreeTableWithAndWithOr() throws Exception {
        for (int i = 0; i < joinType.length; i++) {
            sql = "SELECT * from " + host_info + " a " + joinType[i] + " JOIN " + host_info
                  + " b ON a.host_id=b.host_id " + joinType[i] + " JOIN " + host_info
                  + " c ON b.host_id=c.host_id and b.host_id=1 and c.host_id=1 where a.host_id=1";
            if (!host_info.contains("oneGroup_oneAtom")) sql = hint + sql;
            String[] columnParam = { "host_id", "host_name" };
            selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
        }
    }

    @Test
    public void AndAndTest() throws Exception {
        for (int i = 0; i < innerJoin.length; i++) {
            sql = "select " + host_info + ".host_id," + "" + host_info + ".host_name," + host_info + ".hostgroup_id,"
                  + hostgroup_info + ".hostgroup_name " + "from " + hostgroup_info + " " + joinType[i] + " join "
                  + host_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup_info + ".hostgroup_id where "
                  + host_info + ".host_name='hostname80' and " + hostgroup_info + ".hostgroup_name='hostgroupname80'";
            if (!host_info.contains("oneGroup_oneAtom")) sql = hint + sql;
            selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
        }
    }

    @Test
    public void AndAndSameFiledTest() throws Exception {
        for (int i = 0; i < innerJoin.length; i++) {
            sql = "select " + host_info + ".host_id," + "" + host_info + ".host_name," + host_info + ".hostgroup_id,"
                  + hostgroup_info + ".hostgroup_name " + "from " + hostgroup_info + " " + joinType[i] + " join "
                  + host_info + "  " + "on " + host_info + ".hostgroup_id=" + hostgroup_info + ".hostgroup_id where "
                  + hostgroup_info + ".hostgroup_id>20 and " + hostgroup_info + ".hostgroup_id<80";
            if (!host_info.contains("oneGroup_oneAtom")) sql = hint + sql;
            selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
        }
    }
}
