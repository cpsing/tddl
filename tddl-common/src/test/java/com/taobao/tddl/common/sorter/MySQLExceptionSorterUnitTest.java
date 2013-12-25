package com.taobao.tddl.common.sorter;

import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.common.jdbc.sorter.MySQLExceptionSorter;

/**
 * @author yangzhu
 */
public class MySQLExceptionSorterUnitTest {

    @Test
    public void all() {
        MySQLExceptionSorter s = new MySQLExceptionSorter();
        int[] errors = new int[] { 1040, 1042, 1043, 1047, 1081, 1129, 1130, 1045, 1004, 1005, 1021, 1041, 1037, 1038 };

        SQLException e = new SQLException("reason", "08S01");

        Assert.assertTrue(s.isExceptionFatal(e));

        for (int err : errors) {
            e = new SQLException("reason", "01XXX", err);
            Assert.assertTrue(s.isExceptionFatal(e));
        }

        e = new SQLException("no datasource!", "01XXX");
        Assert.assertTrue(s.isExceptionFatal(e));
        e = new SQLException("no alive datasource", "01XXX");
        Assert.assertTrue(s.isExceptionFatal(e));

        e = new SQLException("msg", "01XXX", -1);
        Assert.assertFalse(s.isExceptionFatal(e));
    }
}
