package com.taobao.tddl.qatest.ibatis;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Time;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.ibatis.sqlmap.client.SqlMapClient;
import com.taobao.tddl.qatest.util.NormalTblNullRow;
import com.taobao.tddl.qatest.util.NormalTblRow;

/**
 * Comment for ibatisTypeTest
 * <p/>
 * Author By: zhuoxue.yll Created Date: 2013-4-7 下午4:54:30
 */

public class ibatisTypeTest {

    protected static SqlMapClient andorSqlMapClient          = null;
    // protected static SqlMapClient andorTDHSSqlMapClient = null;
    protected static SqlMapClient mysqlSqlMapClient          = null;
    protected static final String MATRIX_IBATIS_CONTEXT_PATH = "classpath:spring/spring_context.xml";
    private NormalTblRow          row                        = null;
    private NormalTblRow          rowTdhs                    = null;
    private NormalTblRow          rowMysql                   = null;

    @BeforeClass
    public static void setUp() {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(MATRIX_IBATIS_CONTEXT_PATH);
        andorSqlMapClient = (SqlMapClient) context.getBean("ibatis_ds");
        // andorTDHSSqlMapClient = (SqlMapClient)
        // context.getBean("ibatis_tdhs_ds");
        mysqlSqlMapClient = (SqlMapClient) context.getBean("ibatis_mysql");
    }

    @Test
    public void seletctTypeTest() throws SQLException {
        Map<Object, Object> map = new HashMap<Object, Object>();
        map.put("pk", 1);
        // andor的mysql
        List list = andorSqlMapClient.queryForList("select_test", map);
        row = (NormalTblRow) list.get(0);
        List listMysql = mysqlSqlMapClient.queryForList("select_test", map);
        rowMysql = (NormalTblRow) listMysql.get(0);
        rowEquals(row, rowMysql);
        // andor的tdhs
        // List listTdhs =
        // andorTDHSSqlMapClient.queryForList("select_tdhs_test", map);
        // rowTdhs = (NormalTblRow) list.get(0);
        // rowEquals(rowTdhs, rowMysql);
    }

    @Test
    public void selectTypeWithNullTest() throws SQLException {
        Map<Object, Object> map = new HashMap<Object, Object>();
        map.put("pk", 11);
        // andor的mysql
        List listMysql = mysqlSqlMapClient.queryForList("select_test_null", map);
        NormalTblNullRow rowMysql = (NormalTblNullRow) listMysql.get(0);
        List list = andorSqlMapClient.queryForList("select_test_null", map);
        NormalTblNullRow row = (NormalTblNullRow) list.get(0);
        rowNullEquals(row, rowMysql);
        // andor的tdhs
        // List listTdhs =
        // andorTDHSSqlMapClient.queryForList("select_tdhs_null", map);
        // NormalTblNullRow rowTdhs = (NormalTblNullRow) list.get(0);
        // rowNullEquals(rowTdhs, rowMysql);
    }

    @Test
    public void insertTest() throws SQLException {
        long pk = 122345l;
        String s = "abc";
        Byte b = '1';
        int i = 11;
        BigDecimal big = new BigDecimal(11);
        Date da = new Date();
        Time ti = new Time(111111);
        Boolean bl = false;
        Float f = 0.1f;
        Double d = 0.12d;
        Map<Object, Object> map = new HashMap<Object, Object>();
        map.put("pk", pk);
        map.put("varcharr", s);
        map.put("charr", s);
        map.put("blobr", b);
        map.put("integerr", pk);
        map.put("tinyintr", i);
        map.put("smallintr", i);
        map.put("mediumintr", i);
        map.put("bigintr", i);
        map.put("bitr", bl);
        map.put("floatr", f);
        map.put("doubler", d);
        map.put("decimalr", big);
        map.put("dater", da);
        map.put("timer", ti);
        map.put("datetimer", da);
        map.put("timestampr", da);
        map.put("yearr", 2012);

        andorSqlMapClient.delete("delete_test", map);
        mysqlSqlMapClient.delete("delete_test", map);
        // andorTDHSSqlMapClient.delete("delete_tdhs_test", map);

        andorSqlMapClient.insert("insert_test", map);
        mysqlSqlMapClient.insert("insert_test", map);
        // andorTDHSSqlMapClient.insert("insert_tdhs_test", map);

        List list = andorSqlMapClient.queryForList("select_test", map);
        row = (NormalTblRow) list.get(0);
        List listMysql = mysqlSqlMapClient.queryForList("select_test", map);
        rowMysql = (NormalTblRow) listMysql.get(0);
        rowEquals(row, rowMysql);
        // andor的tdhs
        // List listTdhs =
        // andorTDHSSqlMapClient.queryForList("select_tdhs_test", map);
        // rowTdhs = (NormalTblRow) list.get(0);
        // rowEquals(rowTdhs, rowMysql);

        andorSqlMapClient.delete("delete_test", map);
        mysqlSqlMapClient.delete("delete_test", map);
        // andorTDHSSqlMapClient.insert("delete_tdhs_test", map);

    }

    private void rowEquals(NormalTblRow row, NormalTblRow rowMysql) {
        Assert.assertEquals(rowMysql.getPk(), row.getPk());
        Assert.assertEquals(rowMysql.getPkInteger(), row.getPkInteger());

        Assert.assertEquals(rowMysql.getCharr(), row.getCharr());
        Assert.assertEquals(rowMysql.getVarcharr(), row.getVarcharr());
        Assert.assertEquals(rowMysql.getBigintr(), row.getBigintr());
        Assert.assertEquals(rowMysql.getBitr(), row.getBitr());
        if (rowMysql.getBlobr() != null) {
            Assert.assertEquals(rowMysql.getBlobr()[0], row.getBlobr()[0]);
        } else {
            Assert.assertEquals(rowMysql.getBlobr(), row.getBlobr());
        }
        Assert.assertEquals(rowMysql.getDater(), row.getDater());
        Assert.assertEquals(rowMysql.getDateString(), row.getDateString());
        Assert.assertEquals(rowMysql.getDatetimer(), row.getDatetimer());
        Assert.assertEquals(rowMysql.getDecimalr(), row.getDecimalr());
        Assert.assertEquals(rowMysql.getDecimalLong(), row.getDecimalLong());
        Assert.assertEquals(rowMysql.getDecimalInt(), row.getDecimalInt());
        Assert.assertEquals(rowMysql.getDoubler(), row.getDoubler());
        Assert.assertEquals(rowMysql.getDoubleFloat(), row.getDoubleFloat());
        Assert.assertEquals(rowMysql.getFloatr(), row.getFloatr());
        Assert.assertEquals(rowMysql.getFloatDouble(), row.getFloatDouble());
        Assert.assertEquals(rowMysql.getIntegerr(), row.getIntegerr());
        Assert.assertEquals(rowMysql.getMediumintr(), row.getMediumintr());
        Assert.assertEquals(rowMysql.getSmallintr(), row.getSmallintr());
        Assert.assertEquals(rowMysql.getTimer(), row.getTimer());
        Assert.assertEquals(rowMysql.getTimestampr(), row.getTimestampr());
        Assert.assertEquals(rowMysql.getTinyintr(), row.getTinyintr());
        Assert.assertEquals(rowMysql.getYearr(), row.getYearr());
        Assert.assertEquals(rowMysql.getIntegerLong(), row.getIntegerLong());
        Assert.assertEquals(rowMysql.getBigintLong(), row.getBigintLong());
        Assert.assertEquals(rowMysql.getDatetimeDate(), row.getDatetimeDate());

    }

    private void rowNullEquals(NormalTblNullRow row, NormalTblNullRow rowMysql) {
        Assert.assertEquals(rowMysql.getPk(), row.getPk());
        Assert.assertEquals(rowMysql.getPkInteger(), row.getPkInteger());

        Assert.assertEquals(rowMysql.getCharr(), row.getCharr());
        Assert.assertEquals(rowMysql.getVarcharr(), row.getVarcharr());
        Assert.assertEquals(rowMysql.getBigintr(), row.getBigintr());
        Assert.assertEquals(rowMysql.getBitr(), row.getBitr());
        if (rowMysql.getBlobr() != null) {
            Assert.assertEquals(rowMysql.getBlobr()[0], row.getBlobr()[0]);
        } else {
            Assert.assertEquals(rowMysql.getBlobr(), row.getBlobr());
        }
        Assert.assertEquals(rowMysql.getDater(), row.getDater());
        Assert.assertEquals(rowMysql.getDateString(), row.getDateString());
        Assert.assertEquals(rowMysql.getDatetimer(), row.getDatetimer());
        Assert.assertEquals(rowMysql.getDecimalr(), row.getDecimalr());
        Assert.assertEquals(rowMysql.getDoubler(), row.getDoubler());
        Assert.assertEquals(rowMysql.getDoubleFloat(), row.getDoubleFloat());
        Assert.assertEquals(rowMysql.getFloatr(), row.getFloatr());
        Assert.assertEquals(rowMysql.getFloatDouble(), row.getFloatDouble());
        Assert.assertEquals(rowMysql.getIntegerr(), row.getIntegerr());
        Assert.assertEquals(rowMysql.getMediumintr(), row.getMediumintr());
        Assert.assertEquals(rowMysql.getSmallintr(), row.getSmallintr());
        Assert.assertEquals(rowMysql.getTimer(), row.getTimer());
        Assert.assertEquals(rowMysql.getTimestampr(), row.getTimestampr());
        Assert.assertEquals(rowMysql.getTinyintr(), row.getTinyintr());
        Assert.assertEquals(rowMysql.getYearr(), row.getYearr());
        Assert.assertEquals(rowMysql.getDatetimeDate(), row.getDatetimeDate());
    }
}
