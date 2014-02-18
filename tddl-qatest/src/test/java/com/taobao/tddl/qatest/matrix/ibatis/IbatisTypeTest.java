package com.taobao.tddl.qatest.matrix.ibatis;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Time;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.ibatis.sqlmap.client.SqlMapClient;
import com.taobao.tddl.qatest.util.NormalTblRow;

/**
 * Comment for ibatisTypeTest
 * <p/>
 * Author By: zhuoxue.yll Created Date: 2013-4-7 下午4:54:30
 */

public class IbatisTypeTest {

    protected static SqlMapClient andorSqlMapClient          = null;
    // protected static SqlMapClient andorTDHSSqlMapClient = null;
    protected static SqlMapClient mysqlSqlMapClient          = null;
    protected static final String MATRIX_IBATIS_CONTEXT_PATH = "classpath:spring/spring_context.xml";
    private NormalTblRow          row                        = null;
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
        prepareOneRowData();
        Map<Object, Object> map = new HashMap<Object, Object>();
        map.put("pk", 122345l);
        // andor的mysql
        List listMysql = mysqlSqlMapClient.queryForList("select_test", map);
        rowMysql = (NormalTblRow) listMysql.get(0);
        List list = andorSqlMapClient.queryForList("select_test", map);
        row = (NormalTblRow) list.get(0);
        rowEquals(row, rowMysql);
        // andor的tdhs
        // List listTdhs =
        // andorTDHSSqlMapClient.queryForList("select_tdhs_test", map);
        // rowTdhs = (NormalTblRow) list.get(0);
        // rowEquals(rowTdhs, rowMysql);
    }

    @Test
    public void selectTypeWithNullTest() throws SQLException {
        prepareOneRowData();
        Map<Object, Object> map = new HashMap<Object, Object>();
        map.put("pk", 122345l);
        // andor的mysql
        List listMysql = mysqlSqlMapClient.queryForList("select_test", map);
        NormalTblRow rowMysql = (NormalTblRow) listMysql.get(0);
        List list = andorSqlMapClient.queryForList("select_test", map);
        NormalTblRow row = (NormalTblRow) list.get(0);
        rowEquals(row, rowMysql);
        // andor的tdhs
        // List listTdhs =
        // andorTDHSSqlMapClient.queryForList("select_tdhs_null", map);
        // NormalTblNullRow rowTdhs = (NormalTblNullRow) list.get(0);
        // rowNullEquals(rowTdhs, rowMysql);
    }

    private void prepareOneRowData() throws SQLException {
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
        map.put("unsignedintr", 1000);

        andorSqlMapClient.delete("delete_test", map);
        mysqlSqlMapClient.delete("delete_test", map);

        andorSqlMapClient.insert("insert_test", map);
        mysqlSqlMapClient.insert("insert_test", map);
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
        map.put("unsignedintr", 1000);

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

    @Test
    public void boundaryValue_最大值Test() throws SQLException {
        String s = RandomStringUtils.randomAlphanumeric(255);
        byte[] b = s.getBytes();
        BigDecimal big = new BigDecimal(new String("18446744073709551616.99999"));
        String da = "2015-12-31 12:59:59";
        String ti = "12:59:59";
        Boolean bl = true;
        Float f = 1.1f;
        Double d = 1.1d;
        Map<Object, Object> map = new HashMap<Object, Object>();
        map.put("pk", 1);
        map.put("varcharr", s);
        map.put("charr", s);
        map.put("varbinaryr", s);
        map.put("binaryr", s);
        map.put("blobr", b);
        map.put("textr", s);
        map.put("tinyintr", 127);
        map.put("smallintr", 32767);
        map.put("mediumintr", 8388607);
        map.put("integerr", 2147483647);
        map.put("bigintr", 9223372036854775807l);
        map.put("utinyintr", 255);
        map.put("usmallintr", 65535);
        map.put("umediumintr", 16777215);
        map.put("uintegerr", 4294967295l);
        map.put("ubigintr", new BigDecimal("18446744073709551615"));
        map.put("bitr", bl);
        map.put("floatr", f);
        map.put("doubler", d);
        map.put("decimalr", big);
        map.put("dater", da);
        map.put("timer", ti);
        map.put("datetimer", da);
        map.put("timestampr", da);
        map.put("yearr", 40);

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

        map.put("pk", 2);

        andorSqlMapClient.delete("delete_test", map);
        mysqlSqlMapClient.delete("delete_test", map);
        // andorTDHSSqlMapClient.delete("delete_tdhs_test", map);

        andorSqlMapClient.insert("insert_test", map);
        mysqlSqlMapClient.insert("insert_test", map);
        // andorTDHSSqlMapClient.insert("insert_tdhs_test", map);

        list = andorSqlMapClient.queryForList("select_test", map);
        row = (NormalTblRow) list.get(0);
        listMysql = mysqlSqlMapClient.queryForList("select_test", map);
        rowMysql = (NormalTblRow) listMysql.get(0);
        rowEquals(row, rowMysql);

        // andor的tdhs
        // List listTdhs =
        // andorTDHSSqlMapClient.queryForList("select_tdhs_test", map);
        // rowTdhs = (NormalTblRow) list.get(0);
        // rowEquals(rowTdhs, rowMysql);

        Map idsMap = new HashMap();
        idsMap.put("pks", Arrays.asList(1, 2));
        list = andorSqlMapClient.queryForList("select_test_max", idsMap);
        row = (NormalTblRow) list.get(0);
        listMysql = mysqlSqlMapClient.queryForList("select_test_max", idsMap);
        rowMysql = (NormalTblRow) listMysql.get(0);
        rowEquals(row, rowMysql);

        map.put("pk", 1);
        andorSqlMapClient.delete("delete_test", map);
        mysqlSqlMapClient.delete("delete_test", map);
        // andorTDHSSqlMapClient.insert("delete_tdhs_test", map);

        map.put("pk", 2);
        andorSqlMapClient.delete("delete_test", map);
        mysqlSqlMapClient.delete("delete_test", map);
        // andorTDHSSqlMapClient.insert("delete_tdhs_test", map);
    }

    @Test
    public void boundaryValue_最小值Test() throws SQLException {
        String s = RandomStringUtils.randomAlphanumeric(255);
        byte[] b = s.getBytes();
        BigDecimal big = new BigDecimal(new String("-18446744073709551616.99999"));
        String da = "2015-12-31 12:59:59";
        String ti = "12:59:59";
        Boolean bl = true;
        Float f = -1.1f;
        Double d = -1.1d;
        Map<Object, Object> map = new HashMap<Object, Object>();
        map.put("pk", Integer.MIN_VALUE);
        map.put("varcharr", s);
        map.put("charr", s);
        map.put("varbinaryr", s);
        map.put("binaryr", s);
        map.put("blobr", b);
        map.put("textr", s);
        map.put("tinyintr", -128);
        map.put("smallintr", -32768);
        map.put("mediumintr", -8388608);
        map.put("integerr", -2147483648);
        map.put("bigintr", -9223372036854775808l);
        map.put("utinyintr", 1);
        map.put("usmallintr", 1);
        map.put("umediumintr", 1);
        map.put("uintegerr", 1);
        map.put("ubigintr", new BigDecimal("1"));
        map.put("bitr", bl);
        map.put("floatr", f);
        map.put("doubler", d);
        map.put("decimalr", big);
        map.put("dater", da);
        map.put("timer", ti);
        map.put("datetimer", da);
        map.put("timestampr", da);
        map.put("yearr", 40);

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

        map.put("pk", 2);

        andorSqlMapClient.delete("delete_test", map);
        mysqlSqlMapClient.delete("delete_test", map);
        // andorTDHSSqlMapClient.delete("delete_tdhs_test", map);

        andorSqlMapClient.insert("insert_test", map);
        mysqlSqlMapClient.insert("insert_test", map);
        // andorTDHSSqlMapClient.insert("insert_tdhs_test", map);

        list = andorSqlMapClient.queryForList("select_test", map);
        row = (NormalTblRow) list.get(0);
        listMysql = mysqlSqlMapClient.queryForList("select_test", map);
        rowMysql = (NormalTblRow) listMysql.get(0);
        rowEquals(row, rowMysql);

        // andor的tdhs
        // List listTdhs =
        // andorTDHSSqlMapClient.queryForList("select_tdhs_test", map);
        // rowTdhs = (NormalTblRow) list.get(0);
        // rowEquals(rowTdhs, rowMysql);

        Map idsMap = new HashMap();
        idsMap.put("pks", Arrays.asList(1, 2));
        list = andorSqlMapClient.queryForList("select_test_min", idsMap);
        row = (NormalTblRow) list.get(0);
        listMysql = mysqlSqlMapClient.queryForList("select_test_min", idsMap);
        rowMysql = (NormalTblRow) listMysql.get(0);
        rowEquals(row, rowMysql);

        map.put("pk", 1);
        andorSqlMapClient.delete("delete_test", map);
        mysqlSqlMapClient.delete("delete_test", map);
        // andorTDHSSqlMapClient.insert("delete_tdhs_test", map);

        map.put("pk", 2);
        andorSqlMapClient.delete("delete_test", map);
        mysqlSqlMapClient.delete("delete_test", map);
        // andorTDHSSqlMapClient.insert("delete_tdhs_test", map);
    }

    private void rowEquals(NormalTblRow row, NormalTblRow rowMysql) {
        Assert.assertEquals(rowMysql.getPk(), row.getPk());
        Assert.assertEquals(rowMysql.getCharr(), row.getCharr());
        Assert.assertEquals(rowMysql.getVarcharr(), row.getVarcharr());
        Assert.assertEquals(rowMysql.getVarbinaryr(), row.getVarbinaryr());
        Assert.assertEquals(rowMysql.getBinaryr(), row.getBinaryr());
        Assert.assertArrayEquals(rowMysql.getBlobr(), row.getBlobr());
        Assert.assertEquals(rowMysql.getTextr(), row.getTextr());
        Assert.assertEquals(rowMysql.getTinyintr(), row.getTinyintr());
        Assert.assertEquals(rowMysql.getSmallintr(), row.getSmallintr());
        Assert.assertEquals(rowMysql.getMediumintr(), row.getMediumintr());
        Assert.assertEquals(rowMysql.getIntegerr(), row.getIntegerr());
        Assert.assertEquals(rowMysql.getBigintr(), row.getBigintr());
        Assert.assertEquals(rowMysql.getUtinyintr(), row.getUtinyintr());
        Assert.assertEquals(rowMysql.getUsmallintr(), row.getUsmallintr());
        Assert.assertEquals(rowMysql.getUmediumintr(), row.getUmediumintr());
        Assert.assertEquals(rowMysql.getUintegerr(), row.getUintegerr());
        Assert.assertEquals(rowMysql.getUbigintr(), row.getUbigintr());
        Assert.assertEquals(rowMysql.getBitr(), row.getBitr());

        Assert.assertEquals(rowMysql.getDater().toString(), row.getDater().toString());
        Assert.assertEquals(rowMysql.getDateString().toString(), row.getDateString().toString());
        Assert.assertEquals(rowMysql.getDatetimer().toString(), row.getDatetimer().toString());
        Assert.assertEquals(rowMysql.getDecimalr(), row.getDecimalr());
        Assert.assertEquals(rowMysql.getDoubler(), row.getDoubler());
        Assert.assertEquals(rowMysql.getFloatr(), row.getFloatr());
        Assert.assertEquals(rowMysql.getTimer().toString(), row.getTimer().toString());
        Assert.assertEquals(rowMysql.getTimestampr().toString(), row.getTimestampr().toString());
        Assert.assertEquals(rowMysql.getDatetimeDate().toString(), row.getDatetimeDate().toString());
        Assert.assertEquals(rowMysql.getYearr().toString(), row.getYearr().toString());
    }

}
