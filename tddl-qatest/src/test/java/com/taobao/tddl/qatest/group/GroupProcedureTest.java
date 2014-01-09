package com.taobao.tddl.qatest.group;

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.CallableStatementCallback;

/**
 * 存储过程进行测试
 * <p/>
 * Created Date: 2010-12-10 下午03:28:43
 */
public class GroupProcedureTest extends GroupTestCase {

    @Test
    public void testProcedure() {
        // 调用存过删除moddbtab_0000数据,无参
        String sql = "{call DELETE_DATA()}";

        tddlJT.execute(sql, new CallableStatementCallback() {

            @Override
            public Object doInCallableStatement(CallableStatement arg0) throws SQLException, DataAccessException {
                arg0.execute();
                return null;
            }
        });

        // 调用存过插入100条数据,参数为插入次数
        String sql2 = "{call INSERT_DATA(?)}";
        tddlJT.execute(sql2, new CallableStatementCallback() {

            @Override
            public Object doInCallableStatement(CallableStatement arg0) throws SQLException, DataAccessException {
                arg0.setInt(1, 100);
                arg0.execute();
                return null;
            }
        });

        // 调用存过返回100条数据pk和(1...100),正确结果应该为5050
        String sql3 = "{call GET_PK_SUM_PROC(?)}";
        Object x = tddlJT.execute(sql3, new CallableStatementCallback() {

            @Override
            public Object doInCallableStatement(CallableStatement arg0) throws SQLException, DataAccessException {
                arg0.registerOutParameter(1, Types.INTEGER);
                arg0.execute();
                return arg0.getInt(1);
            }
        });

        Assert.assertEquals(5050, x);

        // 清理数据
        tddlJT.execute(sql, new CallableStatementCallback() {

            @Override
            public Object doInCallableStatement(CallableStatement arg0) throws SQLException, DataAccessException {
                arg0.execute();
                return null;
            }
        });
    }
}
