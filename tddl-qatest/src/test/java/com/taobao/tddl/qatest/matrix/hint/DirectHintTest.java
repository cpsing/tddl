package com.taobao.tddl.qatest.matrix.hint;

import java.util.ArrayList;
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

    public DirectHintTest(){
        BaseTestCase.normaltblTableName = "mysql_normaltbl_oneGroup_oneAtom";
        jdbcTemplate = new JdbcTemplate(us);
    }

    @Before
    public void initData() throws Exception {
        andorUpdateData("delete from  " + normaltblTableName, null);
    }

    @Test
    public void test_指定库_不指定表() throws Exception {
        String sql = "/*+TDDL({\"type\":\"direct\",\"dbid\":\"andor_mysql_group_0\")*/ insert into "
                     + normaltblTableName + " values(?,?,?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(gmt);
        param.add(gmt);
        param.add(gmt);
        param.add(name);
        param.add(fl);
        mysqlUpdateData(sql, param);

        sql = "select gmt_timestamp from " + normaltblTableName + " where pk=" + RANDOM_ID;
        Map re = jdbcTemplate.queryForMap(sql);
        Assert.assertEquals(gmt, String.valueOf(re.get("gmt_create")));
    }
}
