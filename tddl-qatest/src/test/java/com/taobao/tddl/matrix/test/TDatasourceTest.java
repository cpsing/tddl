package com.taobao.tddl.matrix.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.matrix.jdbc.TDataSource;

public class TDatasourceTest {

    @Test
    public void initTestWithDS() throws TddlException, SQLException {

        TDataSource ds = new TDataSource();
        ds.setAppName("andor_show");
        ds.setTopologyFile("test_matrix.xml");
        ds.setSchemaFile("test_schema.xml");
        ds.init();

        Connection conn = ds.getConnection();
        PreparedStatement ps = conn.prepareStatement("select * from bmw_users limit 10");
        ResultSet rs = ps.executeQuery();

        Assert.assertTrue(rs.next());

        rs.close();
        ps.close();
        conn.close();
    }
}
