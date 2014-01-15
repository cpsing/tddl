package com.taobao.tddl.qatest.matrix.hint;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;

public class DirectHintTest extends BaseMatrixTestCase {

    private JdbcTemplate jdbcTemplate;
    private Date         time = new Date();

    public DirectHintTest(){
        BaseTestCase.normaltblTableName = "mysql_normaltbl_oneGroup_oneAtom";
        jdbcTemplate = new JdbcTemplate(us);
    }

    @Before
    public void initData() throws Exception {
        andorUpdateData("delete from mysql_normaltbl_oneGroup_oneAtom", null);
        andorUpdateData("delete from mysql_normaltbl_onegroup_mutilatom", null);
    }

    @Test
    public void test_指定库_不指定表() throws Exception {
        String sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"andor_mysql_group_oneAtom\"})*/";
        sql += "insert into mysql_normaltbl_oneGroup_oneAtom values(?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(time);
        param.add(time);
        param.add(time);
        param.add(name);
        param.add(fl);
        andorUpdateData(sql, param);

        sql = "select gmt_timestamp from " + normaltblTableName + " where pk=" + RANDOM_ID;
        Map re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"andor_mysql_group_oneAtom\"})*/";
        sql += "delete from mysql_normaltbl_oneGroup_oneAtom where pk = " + RANDOM_ID;
        andorUpdateData(sql, null);
    }

    @Test
    public void test_指定库_指定表() throws Exception {
        // 源表为mysql_normaltbl_oneGroup_oneAtom, 指定两个表
        // mysql_normaltbl_onegroup_mutilatom_00 ，
        // mysql_normaltbl_onegroup_mutilatom_01
        String sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"andor_mysql_group_oneAtom\",\"vtab\":\"mysql_normaltbl_oneGroup_oneAtom\",\"realtabs\":[\"mysql_normaltbl_onegroup_mutilatom_00\",\"mysql_normaltbl_onegroup_mutilatom_01\"]})*/ ";
        sql += "insert into mysql_normaltbl_oneGroup_oneAtom values(?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(time);
        param.add(time);
        param.add(time);
        param.add(name);
        param.add(fl);
        andorUpdateData(sql, param);

        // 分别查询两个库
        sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"andor_mysql_group_oneAtom\",\"vtab\":\"mysql_normaltbl_oneGroup_oneAtom\",\"realtabs\":[\"mysql_normaltbl_onegroup_mutilatom_00\"]})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_oneGroup_oneAtom where pk=" + RANDOM_ID;
        Map re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"andor_mysql_group_oneAtom\",\"vtab\":\"mysql_normaltbl_oneGroup_oneAtom\",\"realtabs\":[\"mysql_normaltbl_onegroup_mutilatom_01\"]})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_oneGroup_oneAtom where pk=" + RANDOM_ID;
        re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        // 删除
        sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"andor_mysql_group_oneAtom\",\"vtab\":\"mysql_normaltbl_oneGroup_oneAtom\",\"realtabs\":[\"mysql_normaltbl_onegroup_mutilatom_00\",\"mysql_normaltbl_onegroup_mutilatom_01\"]})*/";
        sql += "delete from mysql_normaltbl_oneGroup_oneAtom where pk = " + RANDOM_ID;
        andorUpdateData(sql, null);
    }

    @Test
    public void test_指定库_指定表_多表名替换() throws Exception {
        String sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"andor_mysql_group_oneAtom\",\"vtab\":\"mysql_normaltbl_oneGroup_oneAtom\",\"realtabs\":[\"mysql_normaltbl_onegroup_mutilatom_00\",\"mysql_normaltbl_onegroup_mutilatom_01\"]})*/";
        sql += "insert into mysql_normaltbl_oneGroup_oneAtom values(?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(time);
        param.add(time);
        param.add(time);
        param.add(name);
        param.add(fl);
        andorUpdateData(sql, param);

        // 多表名替换时，用逗号分隔
        sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"andor_mysql_group_oneAtom\",\"vtab\":\"tablea,tableb\",\"realtabs\":[\"mysql_normaltbl_onegroup_mutilatom_00,mysql_normaltbl_onegroup_mutilatom_01\"]})*/";
        sql += "select a.gmt_timestamp as atime,b.gmt_timestamp as btime from tablea a inner join tableb b on a.pk = b.pk where a.pk="
               + RANDOM_ID;
        Map re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("ATIME")).getTime() / 1000);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("BTIME")).getTime() / 1000);

        // 删除
        sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"andor_mysql_group_oneAtom\",\"vtab\":\"mysql_normaltbl_oneGroup_oneAtom\",\"realtabs\":[\"mysql_normaltbl_onegroup_mutilatom_00\",\"mysql_normaltbl_onegroup_mutilatom_01\"]})*/";
        sql += "delete from mysql_normaltbl_oneGroup_oneAtom where pk = " + RANDOM_ID;
        andorUpdateData(sql, null);
    }

    @Test
    public void test_指定库_指定表_绑定变量() throws Exception {
        // 源表为mysql_normaltbl_oneGroup_oneAtom, 指定两个表
        // mysql_normaltbl_onegroup_mutilatom_00 ，
        // mysql_normaltbl_onegroup_mutilatom_01
        String sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"?\",\"vtab\":\"?\",\"realtabs\":[\"?\",\"?\"]})*/ ";
        sql += "insert into mysql_normaltbl_oneGroup_oneAtom values(?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add("andor_mysql_group_oneAtom");
        param.add("mysql_normaltbl_oneGroup_oneAtom");
        param.add("mysql_normaltbl_onegroup_mutilatom_00");
        param.add("mysql_normaltbl_onegroup_mutilatom_01");
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(time);
        param.add(time);
        param.add(time);
        param.add(name);
        param.add(fl);
        andorUpdateData(sql, param);

        // 分别查询两个库
        sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"?\",\"vtab\":\"?\",\"realtabs\":[\"?\"]})*/ ";
        sql += "select gmt_timestamp from mysql_normaltbl_oneGroup_oneAtom where pk=" + RANDOM_ID;
        Object args0[] = { "andor_mysql_group_oneAtom", "mysql_normaltbl_oneGroup_oneAtom",
                "mysql_normaltbl_onegroup_mutilatom_00" };
        Map re = jdbcTemplate.queryForMap(sql, args0);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"?\",\"vtab\":\"?\",\"realtabs\":[\"?\"]})*/ ";
        sql += "select gmt_timestamp from mysql_normaltbl_oneGroup_oneAtom where pk=" + RANDOM_ID;
        Object args1[] = { "andor_mysql_group_oneAtom", "mysql_normaltbl_oneGroup_oneAtom",
                "mysql_normaltbl_onegroup_mutilatom_01" };
        re = jdbcTemplate.queryForMap(sql, args1);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        // 删除
        sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"?\",\"vtab\":\"?\",\"realtabs\":[\"?\",\"?\"]})*/ ";
        sql += "delete from mysql_normaltbl_oneGroup_oneAtom where pk = " + RANDOM_ID;
        param = new ArrayList<Object>();
        param.add("andor_mysql_group_oneAtom");
        param.add("mysql_normaltbl_oneGroup_oneAtom");
        param.add("mysql_normaltbl_onegroup_mutilatom_00");
        param.add("mysql_normaltbl_onegroup_mutilatom_01");
        andorUpdateData(sql, param);
    }

    @Test
    public void test_指定库_指定表_选择groupindex() throws Exception {
        // 源表为mysql_normaltbl_oneGroup_oneAtom, 指定两个表
        // mysql_normaltbl_onegroup_mutilatom_00 ，
        // mysql_normaltbl_onegroup_mutilatom_01
        String sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"andor_mysql_group_oneAtom\",\"vtab\":\"mysql_normaltbl_oneGroup_oneAtom\",\"realtabs\":[\"mysql_normaltbl_onegroup_mutilatom_00\",\"mysql_normaltbl_onegroup_mutilatom_01\"]})*/ ";
        sql += "/*+TDDL_GROUP({groupIndex:0})*/";
        sql += "insert into mysql_normaltbl_oneGroup_oneAtom values(?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(time);
        param.add(time);
        param.add(time);
        param.add(name);
        param.add(fl);
        andorUpdateData(sql, param);

        // 分别查询两个库
        sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"andor_mysql_group_oneAtom\",\"vtab\":\"mysql_normaltbl_oneGroup_oneAtom\",\"realtabs\":[\"mysql_normaltbl_onegroup_mutilatom_00\"]})*/";
        sql += "/*+TDDL_GROUP({groupIndex:0})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_oneGroup_oneAtom where pk=" + RANDOM_ID;
        Map re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"andor_mysql_group_oneAtom\",\"vtab\":\"mysql_normaltbl_oneGroup_oneAtom\",\"realtabs\":[\"mysql_normaltbl_onegroup_mutilatom_01\"]})*/";
        sql += "/*+TDDL_GROUP({groupIndex:0})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_oneGroup_oneAtom where pk=" + RANDOM_ID;
        re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        // 删除
        sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"andor_mysql_group_oneAtom\",\"vtab\":\"mysql_normaltbl_oneGroup_oneAtom\",\"realtabs\":[\"mysql_normaltbl_onegroup_mutilatom_00\",\"mysql_normaltbl_onegroup_mutilatom_01\"]})*/";
        sql += "/*+TDDL_GROUP({groupIndex:0})*/";
        sql += "delete from mysql_normaltbl_oneGroup_oneAtom where pk = " + RANDOM_ID;
        andorUpdateData(sql, null);
    }
}
