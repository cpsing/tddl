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

@RunWith(EclipseParameterized.class)
public class JoinTest extends BaseTddlTestCase {

    // common fields
    private final long   module_id          = Math.abs(rand.nextLong());
    private final long   hostgroup_id       = Math.abs(rand.nextLong());

    // monitor_module_info fields
    private final long   product_id         = Math.abs(rand.nextLong());
    private final String module_name        = "my_module_name";
    private final long   parent_module_id   = Math.abs(rand.nextLong());
    private final String applevel           = "my_applevel";
    private final String apprisk            = "my_apprisk";
    private final long   sequence           = Math.abs(rand.nextLong());
    private final String module_description = "my_module_description";
    private final String alias_name         = "my_alias_name";

    // monitor_host_info fields
    private final long   host_id            = Math.abs(rand.nextLong());
    private final String host_name          = "my_host_name";
    private final String host_ip            = "my_host_ip";
    private final String host_type_id       = "my_host_type_id";
    private final String station_id         = "my_station_id";
    private final String snmp_community     = "my_snmp_community";
    private final String status             = "my_status";
    private final long   host_flag          = Math.abs(rand.nextLong());

    // monitor_hostgroup_info fields
    private final String gstation_id        = "my_gstation_id";
    private final String hostgroup_name     = "my_hostgroup_name";
    private final long   nagios_id          = Math.abs(rand.nextLong());
    private final long   hostgroup_flag     = Math.abs(rand.nextLong());

    String[]             columnParam        = { "APPNAME", "HOSTIP", "STATIONID", "HOSTGROUPNAME" };

    @Parameters(name = "{index}:table0={0},table1={1},table2={2},table3={3},table4={4}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableName.hostinfoHostgoupStudentModuleinfoModulehostTable(dbType));
    }

    public JoinTest(String monitor_host_infoTableName, String monitor_hostgroup_infoTableName, String studentTableName,
                    String monitor_module_infoTableName, String monitor_module_hostTableName) throws Exception{
        BaseTestCase.host_info = monitor_host_infoTableName;
        BaseTestCase.hostgroup_info = monitor_hostgroup_infoTableName;
        BaseTestCase.studentTableName = studentTableName;
        BaseTestCase.module_info = monitor_module_infoTableName;
        BaseTestCase.module_host = monitor_module_hostTableName;
        init();
    }

    public void init() throws Exception {
        con = getConnection();
        andorCon = us.getConnection();
        andorUpdateData("delete from " + module_info, null);
        andorUpdateData("delete from " + host_info, null);
        andorUpdateData("delete from " + hostgroup_info, null);

        mysqlUpdateData("delete from " + module_info, null);
        mysqlUpdateData("delete from " + host_info, null);
        mysqlUpdateData("delete from " + hostgroup_info, null);
        // insert monitor_module_info
        String sql = "replace into "
                     + module_info
                     + "(module_id, product_id,module_name, parent_module_id, applevel, apprisk, sequence,module_description, alias_name) values(?,?,?,?,?,?,?,?,?)";

        andorUpdateData(sql,
            Arrays.asList(new Object[] { module_id, product_id, module_name, parent_module_id, applevel, apprisk,
                    sequence, module_description, alias_name }));
        mysqlUpdateData(sql,
            Arrays.asList(new Object[] { module_id, product_id, module_name, parent_module_id, applevel, apprisk,
                    sequence, module_description, alias_name }));

        // insert monitor_host_info
        sql = "replace into "
              + host_info
              + "(host_id, host_name, host_ip,host_type_id, hostgroup_id, station_id, snmp_community, status, host_flag) values(?,?,?,?,?,?,?,?,?)";
        andorUpdateData(sql,
            Arrays.asList(new Object[] { host_id, host_name, host_ip, host_type_id, hostgroup_id, station_id,
                    snmp_community, status, host_flag }));
        mysqlUpdateData(sql,
            Arrays.asList(new Object[] { host_id, host_name, host_ip, host_type_id, hostgroup_id, station_id,
                    snmp_community, status, host_flag }));

        // insert monitor_hostgroup_info
        sql = "replace into " + hostgroup_info
              + "(hostgroup_id, station_id,hostgroup_name, module_id, nagios_id, hostgroup_flag) values(?,?,?,?,?,?)";
        andorUpdateData(sql,
            Arrays.asList(new Object[] { hostgroup_id, gstation_id, hostgroup_name, module_id, nagios_id,
                    hostgroup_flag }));

        mysqlUpdateData(sql,
            Arrays.asList(new Object[] { hostgroup_id, gstation_id, hostgroup_name, module_id, nagios_id,
                    hostgroup_flag }));
    }

    @After
    public void destory() throws Exception {
        psConRcRsClose(rc, rs);
    }

    // private static void prepare(String insert, Object[] args) throws
    // Exception {
    // ResultCursor rc = execute(null, insert, Arrays.asList(args));
    // Assert.assertEquals(Integer.valueOf(1), rc.getIngoreTableName(rc.next(),
    // ResultCursor.AFFECT_ROW));
    // Assert.assertNull(rc.getException());
    // rc.close();
    // }

    @Test
    public void testJoinA() throws Exception {
        String sql = "select a.module_name as appname ,a.applevel as applevel, b.host_ip as hostip,b.station_id as stationid,"
                     + "c.hostgroup_name as hostgroupname from "
                     + module_info
                     + " a ,"
                     + ""
                     + host_info
                     + " b ,"
                     + hostgroup_info
                     + " c "
                     + "where a.module_id = c.module_id and c.hostgroup_id = b.hostgroup_id and b.status='"
                     + status
                     + "'";
        String[] columnParam = { "APPNAME", "HOSTIP", "STATIONID", "HOSTGROUPNAME", "applevel" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void testJoinA1() throws Exception {
        String sql = "select a.module_name as appname , b.host_ip as hostip,b.station_id as stationid,"
                     + "c.hostgroup_name as hostgroupname from " + host_info + " b ," + "" + module_info + " a ,"
                     + hostgroup_info + " c "
                     + "where c.module_id = a.module_id and b.hostgroup_id = c.hostgroup_id and a.applevel='"
                     + applevel + "' and c.nagios_id=" + nagios_id;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void testJoinB() throws Exception {
        String sql = "select a.module_name as appname , b.host_ip as hostip,b.station_id as stationid,"
                     + "c.hostgroup_name as hostgroupname from " + module_info + " a ," + "" + host_info + " b ,"
                     + hostgroup_info + " c "
                     + "where c.module_id = a.module_id and b.hostgroup_id = c.hostgroup_id and b.status='" + status
                     + "'";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void testJoinB1() throws Exception {
        String sql = "select a.module_name as appname , b.host_ip as hostip,b.station_id as stationid,"
                     + "c.hostgroup_name as hostgroupname from " + module_info + " a ," + "" + host_info + " b ,"
                     + hostgroup_info + " c "
                     + "where c.module_id = a.module_id and b.hostgroup_id = c.hostgroup_id and b.status='" + status
                     + "' and a.applevel='" + applevel + "' and c.nagios_id=" + nagios_id;

        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void testJoinC() throws Exception {
        // bdb数据库join测试以下测试用例抛出异常，原因目前只支持单值查询
        if (module_info.contains("mysql")) {
            String sql = "select a.module_name as appname , b.host_ip as hostip,b.station_id as stationid,"
                         + "c.hostgroup_name as hostgroupname from " + module_info + " a ," + "" + hostgroup_info
                         + " c, " + host_info + " b "
                         + "where c.module_id = a.module_id and c.hostgroup_id = b.hostgroup_id and b.status='"
                         + status + "'";

            selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
        }
    }

    @Test
    public void testJoinC1() throws Exception {
        // bdb数据库join测试以下测试用例抛出异常，原因目前只支持单值查询
        if (module_info.contains("mysql")) {
            String sql = "select a.module_name as appname , b.host_ip as hostip,b.station_id as stationid,"
                         + "c.hostgroup_name as hostgroupname from " + module_info + " a ," + "" + hostgroup_info
                         + " c, " + host_info + " b "
                         + "where c.module_id = a.module_id and b.hostgroup_id = c.hostgroup_id and b.status='"
                         + status + "' and a.applevel='" + applevel + "' and c.nagios_id=" + nagios_id;

            selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
        }
    }

    @Test
    public void testJoinD() throws Exception {
        // bdb数据库join测试以下测试用例抛出异常，原因目前只支持单值查询
        if (module_info.contains("mysql")) {
            String sql = "select a.module_name as appname , b.host_ip as hostip,b.station_id as stationid,"
                         + "c.hostgroup_name as hostgroupname from " + module_info + " a ," + "" + hostgroup_info
                         + " c, " + host_info + " b "
                         + "where c.module_id = a.module_id and b.hostgroup_id = c.hostgroup_id and b.status='"
                         + status + "'";

            selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
        }
    }

    @Test
    public void testJoinD1() throws Exception {
        // bdb数据库join测试以下测试用例抛出异常，原因目前只支持单值查询
        if (module_info.contains("mysql")) {
            String sql = "select a.module_name as appname , b.host_ip as hostip,b.station_id as stationid,"
                         + "c.hostgroup_name as hostgroupname from " + module_info + " a ," + "" + hostgroup_info
                         + " c, " + host_info + " b "
                         + "where c.module_id = a.module_id and b.hostgroup_id = c.hostgroup_id and b.status='"
                         + status + "' and a.applevel='" + applevel + "' and c.nagios_id=" + nagios_id;

            selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
        }
    }
}
