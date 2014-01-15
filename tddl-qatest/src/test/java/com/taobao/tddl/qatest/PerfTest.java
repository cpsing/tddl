package com.taobao.tddl.qatest;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang.math.RandomUtils;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.matrix.jdbc.TDataSource;

public class PerfTest {

    public static void main(String[] args) throws TddlException, SQLException {
        TDataSource dataSource = new TDataSource();
        dataSource.setAppName("corona_perf_qatest");
        dataSource.init();

        selectMutilStable(dataSource);
    }

    public static boolean selectMutilStable(TDataSource dataSource) throws SQLException {
        String sql = String.format("select auction_type from auction_auctions where auction_id=%s",
            RandomUtils.nextInt(1000000));
        Connection coronaCon = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            coronaCon = dataSource.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        try {
            ps = coronaCon.prepareStatement(sql);
            rs = ps.executeQuery();
            if (rs.next()) {
                System.out.println(rs.getObject(1));
            }
        } catch (SQLException e) {
            ps.close();
            return false;
        } catch (Exception e) {
            ps.close();
            return false;
        } finally {
            if (rs != null) rs.close();
            if (ps != null) ps.close();
            if (coronaCon != null) coronaCon.close();
        }
        return true;
    }

}
