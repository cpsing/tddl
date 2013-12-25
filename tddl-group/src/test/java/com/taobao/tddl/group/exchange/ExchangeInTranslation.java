package com.taobao.tddl.group.exchange;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import com.taobao.tddl.group.jdbc.TGroupDataSource;

public class ExchangeInTranslation {

    private static JdbcTemplate jdbcTemplate = null;

    private static DataSource   dataSource   = null;

    @BeforeClass
    public static void setBeforeClass() {
        TGroupDataSource ds = new TGroupDataSource("tddl_sample_group_0", "tddl_sample");
        ds.init();
        dataSource = ds;
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Test
    public void test() {
        try {
            jdbcTemplate.update("delete from complextbl_0000");
            jdbcTemplate.update("delete from complextbl_0001");
            Connection connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            PreparedStatement ps1 = connection.prepareStatement("insert into complextbl_0000 (id,name) values(?,?)");
            ps1.setObject(1, 10);
            ps1.setObject(2, "TEST");
            ps1.execute();

            System.out.println("stop run");
            // this time exchange and wait untill exchange complete
            connection.rollback();

            PreparedStatement ps2 = connection.prepareStatement("insert into complextbl_0001 (id,name) values(?,?)");
            ps2.setObject(1, 10);
            ps2.setObject(2, "TEST");
            ps2.execute();
            connection.commit();
        } catch (SQLException e) {
            Assert.fail(ExceptionUtils.getFullStackTrace(e));
        } finally {
            jdbcTemplate.update("delete from complextbl_0000");
            jdbcTemplate.update("delete from complextbl_0001");
        }
    }

}
