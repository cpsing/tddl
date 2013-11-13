package com.taobao.tddl.group.config;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import com.taobao.tddl.group.jdbc.TGroupDataSource;

public class GetMetaDataTest {

    private static JdbcTemplate jdbcTemplate = null;

    private static DataSource   dataSource   = null;

    @BeforeClass
    public static void setBeforeClass() {
        TGroupDataSource ds = new TGroupDataSource("JW_AT_TEST_GROUP_W", "JW_AT_TEST2 ");
        ds.init();
        dataSource = ds;
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Test
    public void testSetReadOnly() {
        try {

            Connection connection = dataSource.getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet rs1 = metaData.getTypeInfo();
            while (rs1.next()) {
                System.out.println(rs1.getObject(1));
                System.out.println("Ok");
            }
            rs1.close();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
        }
    }

}
