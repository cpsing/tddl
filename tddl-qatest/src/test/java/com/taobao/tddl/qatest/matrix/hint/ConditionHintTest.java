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

public class ConditionHintTest extends BaseMatrixTestCase {

    private JdbcTemplate jdbcTemplate;
    private Date         time = new Date();

    public ConditionHintTest(){
        BaseTestCase.normaltblTableName = "mysql_normaltbl_oneGroup_oneAtom";
        jdbcTemplate = new JdbcTemplate(us);
    }

    @Before
    public void initData() throws Exception {
        andorUpdateData("delete from mysql_normaltbl_oneGroup_oneAtom", null);
        andorUpdateData("delete from mysql_normaltbl_onegroup_mutilatom", null);
    }

    @Test
    public void test_简单等值条件() throws Exception {
        String sql = "/*+TDDL({\"type\":\"condition\",\"vtab\":\"mysql_normaltbl_onegroup_mutilatom\",\"params\":[{\"expr\":[\"pk=1:int\"]}]})*/";
        sql += "insert into mysql_normaltbl_onegroup_mutilatom values(?,?,?,?,?,?,?)";

        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(time);
        param.add(time);
        param.add(time);
        param.add(name);
        param.add(fl);
        andorUpdateData(sql, param);

        // 用直连库进行查询
        sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"andor_mysql_group_2\",\"vtab\":\"mysql_normaltbl_onegroup_mutilatom\",\"realtabs\":[\"mysql_normaltbl_onegroup_mutilatom_01\"]})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        Map re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        // 继续用规则hint查询
        sql = "/*+TDDL({\"type\":\"condition\",\"vtab\":\"mysql_normaltbl_onegroup_mutilatom\",\"params\":[{\"expr\":[\"pk=1:int\"]}]})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        // 执行删除
        sql = "/*+TDDL({\"type\":\"condition\",\"vtab\":\"mysql_normaltbl_onegroup_mutilatom\",\"params\":[{\"expr\":[\"pk=1:int\"]}]})*/";
        sql += "delete from mysql_normaltbl_onegroup_mutilatom where pk = " + RANDOM_ID;
        andorUpdateData(sql, null);

        // 删除完之后，应该查不到
        sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"andor_mysql_group_2\",\"vtab\":\"mysql_normaltbl_onegroup_mutilatom\",\"realtabs\":[\"mysql_normaltbl_onegroup_mutilatom_01\"]})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        List list = jdbcTemplate.queryForList(sql);
        Assert.assertEquals(0, list.size());
    }

    @Test
    public void test_简单等值条件_指定groupIndex() throws Exception {
        String sql = "/*+TDDL({\"type\":\"condition\",\"vtab\":\"mysql_normaltbl_onegroup_mutilatom\",\"params\":[{\"expr\":[\"pk=1:int\"]}]})*/";
        sql += "/*+TDDL_GROUP({groupIndex:0})*/";
        sql += "insert into mysql_normaltbl_onegroup_mutilatom values(?,?,?,?,?,?,?)";

        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(time);
        param.add(time);
        param.add(time);
        param.add(name);
        param.add(fl);
        andorUpdateData(sql, param);

        // 用直连库进行查询
        sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"andor_mysql_group_2\",\"vtab\":\"mysql_normaltbl_onegroup_mutilatom\",\"realtabs\":[\"mysql_normaltbl_onegroup_mutilatom_01\"]})*/";
        sql += "/*+TDDL_GROUP({groupIndex:0})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        Map re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        // 继续用规则hint查询
        sql = "/*+TDDL({\"type\":\"condition\",\"vtab\":\"mysql_normaltbl_onegroup_mutilatom\",\"params\":[{\"expr\":[\"pk=1:int\"]}]})*/";
        sql += "/*+TDDL_GROUP({groupIndex:0})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        // 执行删除
        sql = "/*+TDDL({\"type\":\"condition\",\"vtab\":\"mysql_normaltbl_onegroup_mutilatom\",\"params\":[{\"expr\":[\"pk=1:int\"]}]})*/";
        sql += "/*+TDDL_GROUP({groupIndex:0})*/";
        sql += "delete from mysql_normaltbl_onegroup_mutilatom where pk = " + RANDOM_ID;
        andorUpdateData(sql, null);

        // 删除完之后，应该查不到
        sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"andor_mysql_group_2\",\"vtab\":\"mysql_normaltbl_onegroup_mutilatom\",\"realtabs\":[\"mysql_normaltbl_onegroup_mutilatom_01\"]})*/";
        sql += "/*+TDDL_GROUP({groupIndex:0})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        List list = jdbcTemplate.queryForList(sql);
        Assert.assertEquals(0, list.size());
    }

    @Test
    public void test_OR条件验证() throws Exception {
        // 使用or条件，会更新两个表
        String sql = "/*+TDDL({\"type\":\"condition\",\"vtab\":\"mysql_normaltbl_onegroup_mutilatom\",\"params\":[{\"relation\":\"or\",\"expr\":[\"pk=0:int\",\"pk=1:int\"]}]})*/";
        sql += "insert into mysql_normaltbl_onegroup_mutilatom values(?,?,?,?,?,?,?)";

        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(time);
        param.add(time);
        param.add(time);
        param.add(name);
        param.add(fl);
        andorUpdateData(sql, param);

        // 用直连库进行查询，查询两个表
        sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"andor_mysql_group_2\",\"vtab\":\"mysql_normaltbl_onegroup_mutilatom\",\"realtabs\":[\"mysql_normaltbl_onegroup_mutilatom_00\"]})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        Map re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"andor_mysql_group_2\",\"vtab\":\"mysql_normaltbl_onegroup_mutilatom\",\"realtabs\":[\"mysql_normaltbl_onegroup_mutilatom_01\"]})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        // 继续用规则hint查询
        sql = "/*+TDDL({\"type\":\"condition\",\"vtab\":\"mysql_normaltbl_onegroup_mutilatom\",\"params\":[{\"relation\":\"or\",\"expr\":[\"pk=0:int\",\"pk=1:int\"]}]})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        List list = jdbcTemplate.queryForList(sql);
        Assert.assertEquals(2, list.size());

        // 执行删除
        sql = "/*+TDDL({\"type\":\"condition\",\"vtab\":\"mysql_normaltbl_onegroup_mutilatom\",\"params\":[{\"relation\":\"or\",\"expr\":[\"pk=0:int\",\"pk=1:int\"]}]})*/";
        sql += "delete from mysql_normaltbl_onegroup_mutilatom where pk = " + RANDOM_ID;
        andorUpdateData(sql, null);

        // 删除完之后，应该查不到
        sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"andor_mysql_group_2\",\"vtab\":\"mysql_normaltbl_onegroup_mutilatom\",\"realtabs\":[\"mysql_normaltbl_onegroup_mutilatom_00\"]})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        list = jdbcTemplate.queryForList(sql);
        Assert.assertEquals(0, list.size());

        sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"andor_mysql_group_2\",\"vtab\":\"mysql_normaltbl_onegroup_mutilatom\",\"realtabs\":[\"mysql_normaltbl_onegroup_mutilatom_01\"]})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        list = jdbcTemplate.queryForList(sql);
        Assert.assertEquals(0, list.size());
    }

    @Test
    public void test_and条件验证() throws Exception {
        // 使用or条件，会更新两个表
        String sql = "/*+TDDL({\"type\":\"condition\",\"vtab\":\"mysql_normaltbl_onegroup_mutilatom\",\"params\":[{\"relation\":\"and\",\"expr\":[\"pk>=0:int\",\"pk<2:int\"]}]})*/";
        sql += "insert into mysql_normaltbl_onegroup_mutilatom values(?,?,?,?,?,?,?)";

        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(time);
        param.add(time);
        param.add(time);
        param.add(name);
        param.add(fl);
        andorUpdateData(sql, param);

        // 用直连库进行查询，查询两个表
        sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"andor_mysql_group_2\",\"vtab\":\"mysql_normaltbl_onegroup_mutilatom\",\"realtabs\":[\"mysql_normaltbl_onegroup_mutilatom_00\"]})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        Map re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"andor_mysql_group_2\",\"vtab\":\"mysql_normaltbl_onegroup_mutilatom\",\"realtabs\":[\"mysql_normaltbl_onegroup_mutilatom_01\"]})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(time.getTime() / 1000, ((Date) re.get("GMT_TIMESTAMP")).getTime() / 1000);

        // 继续用规则hint查询
        sql = "/*+TDDL({\"type\":\"condition\",\"vtab\":\"mysql_normaltbl_onegroup_mutilatom\",\"params\":[{\"relation\":\"and\",\"expr\":[\"pk>=0:int\",\"pk<2:int\"]}]})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        List list = jdbcTemplate.queryForList(sql);
        Assert.assertEquals(2, list.size());

        // 执行删除
        sql = "/*+TDDL({\"type\":\"condition\",\"vtab\":\"mysql_normaltbl_onegroup_mutilatom\",\"params\":[{\"relation\":\"and\",\"expr\":[\"pk>=0:int\",\"pk<2:int\"]}]})*/";
        sql += "delete from mysql_normaltbl_onegroup_mutilatom where pk = " + RANDOM_ID;
        andorUpdateData(sql, null);

        // 删除完之后，应该查不到
        sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"andor_mysql_group_2\",\"vtab\":\"mysql_normaltbl_onegroup_mutilatom\",\"realtabs\":[\"mysql_normaltbl_onegroup_mutilatom_00\"]})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        list = jdbcTemplate.queryForList(sql);
        Assert.assertEquals(0, list.size());

        sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"andor_mysql_group_2\",\"vtab\":\"mysql_normaltbl_onegroup_mutilatom\",\"realtabs\":[\"mysql_normaltbl_onegroup_mutilatom_01\"]})*/";
        sql += "select gmt_timestamp from mysql_normaltbl_onegroup_mutilatom where pk=" + RANDOM_ID;
        list = jdbcTemplate.queryForList(sql);
        Assert.assertEquals(0, list.size());
    }

}
