package com.taobao.tddl.qatest.transaction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseTddlTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.util.EclipseParameterized;
import com.taobao.tddl.qatest.util.ExecuteTableName;

@RunWith(EclipseParameterized.class)
public class TransactionMultiTableTest extends BaseTddlTestCase {

    @Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableName.normaltblStudentTable(dbType));
    }

    public TransactionMultiTableTest(String normaltblTableName, String studentTableName){
        BaseTestCase.normaltblTableName = normaltblTableName;
        BaseTestCase.studentTableName = studentTableName;
    }

    @Before
    public void initData() throws Exception {
        con = getConnection();
        andorCon = us.getConnection();
        andorUpdateData("DELETE FROM " + studentTableName, null);
        mysqlUpdateData("DELETE FROM " + studentTableName, null);
        andorUpdateData("DELETE FROM " + normaltblTableName, null);
        mysqlUpdateData("DELETE FROM " + normaltblTableName, null);
    }

    @After
    public void destory() throws Exception {
        psConRcRsClose(rc, rs);
    }

    @Test
    public void testCommit() throws Exception {
        String sql = "insert into " + normaltblTableName + "(pk,id) values(" + RANDOM_ID + "," + RANDOM_INT + ")";
        andorCon = us.getConnection();
        andorCon.setAutoCommit(false);

        con = getConnection();
        con.setAutoCommit(false);
        try {
            int mysqlAffectRow = mysqlUpdateDataTranscation(sql, null);
            int andorAffectRow = andorUpdateDataTranscation(sql, null);
            Assert.assertEquals(mysqlAffectRow, andorAffectRow);

            sql = "insert into " + studentTableName + " (id,name,school) values (?,?,?)";
            List<Object> param = new ArrayList<Object>();
            param.add(RANDOM_ID);
            param.add(name);
            param.add(school);

            mysqlAffectRow = mysqlUpdateDataTranscation(sql, param);
            andorAffectRow = andorUpdateDataTranscation(sql, param);
            Assert.assertEquals(mysqlAffectRow, andorAffectRow);

            con.commit();
            andorCon.commit();
        } catch (Exception e) {
            try {
                con.rollback();
                andorCon.rollback();
            } catch (Exception ee) {

            }
        }
        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
        String[] columnParam1 = { "ID" };
        selectOrderAssertTranscation(sql, columnParam1, null);
        sql = "select * from " + studentTableName + " where id=" + RANDOM_ID;
        String[] columnParam = { "NAME", "SCHOOL" };
        selectOrderAssertTranscation(sql, columnParam, null);
    }

    @Test
    public void testRollback() throws Exception {
        String sql = "insert into " + normaltblTableName + "(pk,id) values(" + RANDOM_ID + "," + RANDOM_INT + ")";
        andorCon = us.getConnection();
        andorCon.setAutoCommit(false);

        con = getConnection();
        con.setAutoCommit(false);
        try {
            int mysqlAffectRow = mysqlUpdateDataTranscation(sql, null);
            int andorAffectRow = andorUpdateDataTranscation(sql, null);
            Assert.assertEquals(mysqlAffectRow, andorAffectRow);

            sql = "insert into " + studentTableName + " (id,name,school) values (?,?,?)";
            List<Object> param = new ArrayList<Object>();
            param.add(RANDOM_ID);
            param.add(name);
            param.add(school);

            mysqlAffectRow = mysqlUpdateDataTranscation(sql, param);
            andorAffectRow = andorUpdateDataTranscation(sql, param);
            Assert.assertEquals(mysqlAffectRow, andorAffectRow);

            con.rollback();
            andorCon.rollback();
        } catch (Exception e) {
            try {
                con.rollback();
                andorCon.rollback();
            } catch (Exception ee) {

            }
        }
        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
        String[] columnParam1 = { "ID" };
        selectOrderAssertTranscation(sql, columnParam1, null);
        sql = "select * from " + studentTableName + " where id=" + RANDOM_ID;
        String[] columnParam = { "NAME", "SCHOOL" };
        selectOrderAssertTranscation(sql, columnParam, null);

    }

    @Test
    public void testBeforeRollback() throws Exception {
        String sql = "insert into " + normaltblTableName + "(pk,id) values(" + RANDOM_ID + "," + RANDOM_INT + ")";
        andorCon = us.getConnection();
        andorCon.setAutoCommit(false);
        String[] columnParam1 = { "ID" };
        String[] columnParam = { "NAME", "SCHOOL" };
        con = getConnection();
        con.setAutoCommit(false);
        try {
            int mysqlAffectRow = mysqlUpdateDataTranscation(sql, null);
            int andorAffectRow = andorUpdateDataTranscation(sql, null);
            Assert.assertEquals(mysqlAffectRow, andorAffectRow);

            sql = "insert into " + studentTableName + " (id,name,school) values (?,?,?)";
            List<Object> param = new ArrayList<Object>();
            param.add(RANDOM_ID);
            param.add(name);
            param.add(school);

            mysqlAffectRow = mysqlUpdateDataTranscation(sql, param);
            andorAffectRow = andorUpdateDataTranscation(sql, param);
            Assert.assertEquals(mysqlAffectRow, andorAffectRow);
            sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;

            selectOrderAssertTranscation(sql, columnParam1, null);
            sql = "select * from " + studentTableName + " where id=" + RANDOM_ID;

            selectOrderAssertTranscation(sql, columnParam, null);
            con.rollback();
            andorCon.rollback();
        } catch (Exception e) {
            try {
                con.rollback();
                andorCon.rollback();
            } catch (Exception ee) {

            }
        }
        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
        selectOrderAssertTranscation(sql, columnParam1, null);
        sql = "select * from " + studentTableName + " where id=" + RANDOM_ID;
        selectOrderAssertTranscation(sql, columnParam, null);
    }
}
