package com.taobao.tddl.qatest.matrix.select;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.util.EclipseParameterized;
import com.taobao.tddl.qatest.ExecuteTableName;

@Ignore
@RunWith(EclipseParameterized.class)
public class SelectSubQueryTest extends BaseMatrixTestCase {

    String modlueName = "module12";
    long   pk         = 12l;

    @Parameters(name = "{index}:table0={0},table1={1},table2={2},table3={3},table4={4}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableName.hostinfoHostgoupStudentModuleinfoModulehostTable(dbType));
    }

    public SelectSubQueryTest(String monitor_host_infoTableName, String monitor_hostgroup_infoTableName,
                              String studentTableName, String monitor_module_infoTableName,
                              String monitor_module_hostTableName) throws Exception{
        BaseTestCase.host_info = monitor_host_infoTableName;
        BaseTestCase.hostgroup = monitor_hostgroup_infoTableName;
        BaseTestCase.studentTableName = studentTableName;
        BaseTestCase.module_info = monitor_module_infoTableName;
        BaseTestCase.module_host = monitor_module_hostTableName;

    }

    @Before
    public void prepare() throws Exception {
        hostinfoPrepare(0, 20);
        hostgroupPrepare(10, 30);
        module_infoPrepare(0, 40);
    }

    /**
     * 使用子查询进行比较
     * 
     * @throws Exception
     */
    @Test
    public void comparisonTest() throws Exception {
        String sql = "select *  from " + host_info + " where hostgroup_id =(select module_id from " + module_info
                     + " where module_name='" + modlueName + "')";
        String[] columnParam = { "host_id", "host_name", "hostgroup_id" };
        selectOrderAssert(sql, columnParam, null);

        sql = "select *  from " + host_info + " where hostgroup_id <(select module_id from " + module_info
              + " where module_name='" + modlueName + "')";
        selectContentSameAssert(sql, columnParam, null);

        con = getConnection();
        andorCon = us.getConnection();
        sql = "select *  from " + host_info + " where hostgroup_id <=(select module_id from " + module_info
              + " where module_name='" + modlueName + "')";
        selectContentSameAssert(sql, columnParam, null);

        con = getConnection();
        andorCon = us.getConnection();
        sql = "select *  from " + host_info + " where hostgroup_id >(select module_id from " + module_info
              + " where module_name='" + modlueName + "')";
        selectContentSameAssert(sql, columnParam, null);

        con = getConnection();
        andorCon = us.getConnection();
        sql = "select *  from " + host_info + " where hostgroup_id >=(select module_id from " + module_info
              + " where module_name='" + modlueName + "')";
        selectContentSameAssert(sql, columnParam, null);
    }

    /**
     * 使用子查询进行比较,子查询中存在order by ，limit等
     * 
     * @throws Exception
     */
    @Test
    public void comparisonSubWithOrderByLimitTest() throws Exception {
        String sql = "select *  from " + host_info + " where hostgroup_id =(select module_id from " + module_info
                     + " order by module_id limit 1)";
        String[] columnParam = { "host_id", "host_name", "hostgroup_id" };
        selectContentSameAssert(sql, columnParam, null);

        sql = "select *  from " + host_info + " where hostgroup_id >(select module_id from " + module_info
              + " order by module_id  limit 1)";
        selectContentSameAssert(sql, columnParam, null);

        sql = "select *  from " + host_info + " where hostgroup_id >(select module_id from " + module_info
              + " order by module_id asc limit 1)";
        selectContentSameAssert(sql, columnParam, null);

        sql = "select *  from " + host_info + " where hostgroup_id <(select module_id from " + module_info
              + " order by module_id desc limit 1)";
        selectContentSameAssert(sql, columnParam, null);

        sql = "select *  from " + host_info + " where hostgroup_id >(select module_id from " + module_info
              + " where module_name like 'module%' order by  module_id limit 1)";
        selectContentSameAssert(sql, columnParam, null);
    }

    /**
     * 使用子查询进行比较,主查询中存在order by ，limit等
     * 
     * @throws Exception
     */
    @Test
    public void comparisonWithOrderByLimitTest() throws Exception {
        String sql = "select *  from " + host_info + " where hostgroup_id >(select module_id from " + module_info
                     + " where module_name like 'module%' order by  module_id limit 1) order by host_id";
        String[] columnParam = { "host_id", "host_name", "hostgroup_id" };
        selectOrderAssert(sql, columnParam, null);

        sql = "select *  from " + host_info + " where hostgroup_id >(select module_id from " + module_info
              + " where module_name like 'module%' order by  module_id limit 1) order by host_id limit 2";
        selectOrderAssert(sql, columnParam, null);

        sql = "select count(host_id), host_name from "
              + host_info
              + " where hostgroup_id >(select module_id from "
              + module_info
              + " where module_name like 'module%' order by  module_id limit 1) group by  host_name order by host_id limit 2";
        String[] columnParam1 = { "count(host_id)", "host_name" };
        selectOrderAssert(sql, columnParam1, null);
    }

    /**
     * 使用子查询进行比较，子查询中带聚合函数
     * 
     * @throws Exception
     */
    @Test
    public void comparisonWithFuncTest() throws Exception {
        String sql = "select *  from " + host_info + " where hostgroup_id =(select max(hostgroup_id) from " + hostgroup
                     + ")";
        String[] columnParam = { "host_id", "host_name", "hostgroup_id" };
        selectContentSameAssert(sql, columnParam, null);

        sql = "select *  from " + host_info + " where hostgroup_id <=(select max(hostgroup_id) from " + hostgroup + ")";
        selectContentSameAssert(sql, columnParam, null);

        sql = "select *  from " + host_info + " where hostgroup_id =(select min(hostgroup_id) from " + hostgroup + ")";
        selectContentSameAssert(sql, columnParam, null);

        sql = "select *  from " + host_info + " where hostgroup_id =(select avg(hostgroup_id) from " + hostgroup + ")";
        selectContentSameAssert(sql, columnParam, null);

        sql = "select *  from " + host_info + " where hostgroup_id <(select count(*) from " + hostgroup + ")";
        selectContentSameAssert(sql, columnParam, null);
    }

    /**
     * 使用any进行子查询
     * 
     * @throws Exception
     */
    @Test
    public void anyTest() throws Exception {
        String sql = "select * from " + host_info + " where host_id =any(select hostgroup_id from " + hostgroup
                     + " where hostgroup_id>" + pk + ")";
        String[] columnParam = { "host_id", "host_name", "hostgroup_id" };
        selectContentSameAssert(sql, columnParam, null);

        sql = "select * from " + host_info + " where host_id <any(select hostgroup_id from " + hostgroup
              + " where hostgroup_id>" + pk + ")";
        selectContentSameAssert(sql, columnParam, null);

        sql = "select * from " + host_info + " where host_id >any(select hostgroup_id from " + hostgroup
              + " where hostgroup_id>" + pk + ")";
        selectContentSameAssert(sql, columnParam, null);

        sql = "select * from " + host_info + " where host_id <>any(select hostgroup_id from " + hostgroup
              + " where hostgroup_id>" + pk + ")";
        selectContentSameAssert(sql, columnParam, null);
    }

    /**
     * 使用all进子查询
     * 
     * @throws Exception
     */
    @Test
    public void allTest() throws Exception {
        String sql = "select * from " + host_info + " where host_id =ALL(select hostgroup_id from " + hostgroup
                     + " where hostgroup_id>" + pk + ")";
        String[] columnParam = { "host_id", "host_name", "hostgroup_id" };
        selectContentSameAssert(sql, columnParam, null);

        sql = "select * from " + host_info + " where host_id <ALL(select hostgroup_id from " + hostgroup
              + " where hostgroup_id>" + pk + ")";
        selectContentSameAssert(sql, columnParam, null);

        sql = "select * from " + host_info + " where host_id >ALL(select hostgroup_id from " + hostgroup
              + " where hostgroup_id>" + pk + ")";
        selectContentSameAssert(sql, columnParam, null);

        sql = "select * from " + host_info + " where host_id <>ALL(select hostgroup_id from " + hostgroup
              + " where hostgroup_id>" + pk + ")";
        selectContentSameAssert(sql, columnParam, null);
    }

    /**
     * 使用exist 进行子查询
     * 
     * @throws Exception
     */
    @Test
    public void existsTest() throws Exception {
        String sql = "select * from " + host_info + " where EXISTS (select * from " + hostgroup + " where " + host_info
                     + ".hostgroup_id=" + hostgroup + ".hostgroup_id)";
        String[] columnParam = { "host_id", "host_name", "hostgroup_id" };
        selectContentSameAssert(sql, columnParam, null);

        sql = "select * from " + host_info + " where NOT EXISTS (select * from " + hostgroup + " where " + host_info
              + ".hostgroup_id=" + hostgroup + ".hostgroup_id)";
        selectContentSameAssert(sql, columnParam, null);

        sql = "select distinct host_name from " + host_info + " where EXISTS (select * from " + hostgroup + " where "
              + host_info + ".hostgroup_id=" + hostgroup + ".hostgroup_id)";
        String[] columnParam1 = { "host_name" };
        selectContentSameAssert(sql, columnParam1, null);
    }

    /**
     * 关联子查询
     * 
     * @throws Exception
     */
    @Test
    public void associationTest() throws Exception {
        String sql = "select * from " + host_info + " as host where host_id in (select module_id from " + module_info
                     + " as info where host.hostgroup_id=info.module_id)";
        String[] columnParam = { "host_id", "host_name", "hostgroup_id" };
        selectContentSameAssert(sql, columnParam, null);

        sql = "select * from " + host_info + " as host where host_id = (select module_id from " + module_info
              + " as info where host.hostgroup_id=info.module_id)";
        selectContentSameAssert(sql, columnParam, null);
    }

    /**
     * from子句子查询
     * 
     * @throws Exception
     */
    @Test
    public void fromTest() throws Exception {
        String sql = "select host,host_name from (select host_id*2 as host ,host_name from " + host_info + ""
                     + " )as sb where host>" + pk;
        String[] columnParam = { "host", "host_name" };
        selectContentSameAssert(sql, columnParam, null);

        sql = "select avg(sumHost) from (select sum(host_id) as sumHost from " + host_info + ""
              + " group by host_id )as sb";
        String[] columnParam1 = { "avg(sumHost)" };
        selectContentSameAssert(sql, columnParam1, null);
    }

    @Test
    public void CloumnTest() throws Exception {
        String sql = "select host_id ,(select hostgroup_name from " + hostgroup + " where hostgroup_id =" + pk
                     + ")as name from " + host_info;
        String[] columnParam = { "host_id", "name" };
        selectContentSameAssert(sql, columnParam, null);

        sql = "select host_id ,(select hostgroup_name from " + hostgroup
              + " where hostgroup_name like'hostgroupname%' " + "group by hostgroup_name order by hostgroup_id limit 1"
              + " )as name from " + host_info;
        selectContentSameAssert(sql, columnParam, null);
    }

}
