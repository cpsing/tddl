package com.taobao.tddl.qatest.matrix.select.function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.ExecuteTableName;
import com.taobao.tddl.qatest.util.EclipseParameterized;

@RunWith(EclipseParameterized.class)
public class SelectDateFunctionTest extends BaseMatrixTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.normaltblTable(dbType));
    }

    public SelectDateFunctionTest(String tableName){
        BaseTestCase.normaltblTableName = tableName;
    }

    @Before
    public void prepare() throws Exception {
        con = getConnection();
        andorCon = us.getConnection();
        normaltblPrepare(0, 20);
    }

    @After
    public void destory() throws Exception {
        psConRcRsClose(rc, rs);
    }

    @Test
    public void to_daysTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select id,name from " + normaltblTableName
                         + " where TO_DAYS(gmt_create)-TO_DAYS('2011-05-15')>30";
            rs = mysqlQueryData(sql, null);
            rc = andorQueryData(sql, null);

            String[] columnParam1 = { "id", "name" };
            assertContentSame(rs, rc, columnParam1);

            sql = "select TO_DAYS(gmt_create) as da,name from " + normaltblTableName + " where pk=1";

            rs = mysqlQueryData(sql, null);
            rc = andorQueryData(sql, null);

            String[] columnParam2 = { "da", "name" };
            assertContentSame(rs, rc, columnParam2);
        }
    }

    @Test
    public void from_daysTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select FROM_DAYS(TO_DAYS(gmt_create)) as da from " + normaltblTableName + " where pk=0";

            rs = mysqlQueryData(sql, null);
            rc = andorQueryData(sql, null);

            String[] columnParam1 = { "da" };
            assertContentSame(rs, rc, columnParam1);
        }

    }

    @Test
    public void dayofweekTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select DAYOFWEEK(gmt_create)  as da from " + normaltblTableName + " where pk=0";
            rs = mysqlQueryData(sql, null);
            rc = andorQueryData(sql, null);

            String[] columnParam1 = { "da" };
            assertContentSame(rs, rc, columnParam1);
        }
    }

    @Test
    public void weekdayTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select WEEKDAY(gmt_create)  as da from " + normaltblTableName + " where pk=0";

            rs = mysqlQueryData(sql, null);
            rc = andorQueryData(sql, null);

            String[] columnParam1 = { "da" };
            assertContentSame(rs, rc, columnParam1);
        }
    }

    @Test
    public void dayofyearTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select DAYOFYEAR(gmt_create)  as da from " + normaltblTableName + " where pk=0";

            rs = mysqlQueryData(sql, null);
            rc = andorQueryData(sql, null);

            String[] columnParam1 = { "da" };
            assertContentSame(rs, rc, columnParam1);
        }
    }

    @Test
    public void monthTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select MONTH(gmt_create) as da from " + normaltblTableName + " where pk=0";

            rs = mysqlQueryData(sql, null);
            rc = andorQueryData(sql, null);

            String[] columnParam1 = { "da" };
            assertContentSame(rs, rc, columnParam1);
        }
    }

    @Test
    public void monthnameTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select MONTHNAME(gmt_create)  as da from " + normaltblTableName + " where pk=0";

            rs = mysqlQueryData(sql, null);
            rc = andorQueryData(sql, null);

            String[] columnParam1 = { "da" };
            assertContentSame(rs, rc, columnParam1);
        }
    }

    @Test
    public void quarterTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select QUARTER(gmt_create)  as da from " + normaltblTableName + " where pk=0";

            rs = mysqlQueryData(sql, null);
            rc = andorQueryData(sql, null);

            String[] columnParam1 = { "da" };
            assertContentSame(rs, rc, columnParam1);
        }
    }

    @Test
    public void weekTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select WEEK(gmt_create) as da from " + normaltblTableName + " where pk=0";

            rs = mysqlQueryData(sql, null);
            rc = andorQueryData(sql, null);

            String[] columnParam1 = { "da" };
            assertContentSame(rs, rc, columnParam1);
        }
    }

    @Test
    public void timeTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String[] columnParam1 = { "da" };
            String sql = "select YEAR(gmt_create)  as da from " + normaltblTableName + " where pk=0";

            rs = mysqlQueryData(sql, null);
            rc = andorQueryData(sql, null);

            assertContentSame(rs, rc, columnParam1);

            sql = "select HOUR(gmt_create)  as da from " + normaltblTableName + " where pk=0";

            rs = mysqlQueryData(sql, null);
            rc = andorQueryData(sql, null);
            assertContentSame(rs, rc, columnParam1);

            sql = "select MINUTE(gmt_create) as da from " + normaltblTableName + " where pk=0";

            rs = mysqlQueryData(sql, null);
            rc = andorQueryData(sql, null);
            assertContentSame(rs, rc, columnParam1);

            sql = "select SECOND(gmt_create) as da from " + normaltblTableName + " where pk=0";

            rs = mysqlQueryData(sql, null);
            rc = andorQueryData(sql, null);
            assertContentSame(rs, rc, columnParam1);
        }
    }

    @Test
    public void daynameTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select DAYNAME(gmt_create) as da from " + normaltblTableName + " where pk=0";

            rs = mysqlQueryData(sql, null);
            rc = andorQueryData(sql, null);

            String[] columnParam1 = { "da" };
            assertContentSame(rs, rc, columnParam1);
        }
    }

    @Test
    public void dayofmonthTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select DAYOFMONTH(gmt_create) as da from " + normaltblTableName + " where pk=0";

            rs = mysqlQueryData(sql, null);
            rc = andorQueryData(sql, null);

            String[] columnParam1 = { "da" };
            assertContentSame(rs, rc, columnParam1);
        }
    }

    @Test
    public void date_subTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select DATE_SUB(gmt_create,INTERVAL 31 DAY)  as da from " + normaltblTableName
                         + " where pk=1";

            rs = mysqlQueryData(sql, null);
            rc = andorQueryData(sql, null);

            String[] columnParam1 = { "da" };
            assertContentSame(rs, rc, columnParam1);
        }

    }

    public void date_subTestBindVal() throws Exception {
        String sql = "select DATE_SUB(gmt_create,INTERVAL ? DAY)  as da from " + normaltblTableName + " where pk=1";
        List<Object> param = new ArrayList<Object>();
        param.add(31);
        rs = mysqlQueryData(sql, param);
        rc = andorQueryData(sql, param);

        String[] columnParam1 = { "da" };
        assertContentSame(rs, rc, columnParam1);

    }

    @Test
    public void date_addTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select DATE_ADD(gmt_create,INTERVAL 31 DAY) as da from " + normaltblTableName + " where pk=1";
            String[] columnParam = { "da" };
            selectContentSameAssert(sql, columnParam, null);

            sql = "select ADDDATE(gmt_create,INTERVAL 31 DAY) as da from " + normaltblTableName + " where pk=1";
            selectContentSameAssert(sql, columnParam, null);

            sql = "select * from " + normaltblTableName + " where gmt_create > DATE_ADD(Date(?),INTERVAL 31 DAY)";
            List<Object> param = new ArrayList<Object>();
            param.add("2011-5-5");
            String[] columnParam1 = { "name", "id", "pk" };
            selectContentSameAssert(sql, columnParam1, param);

            sql = "select * from " + normaltblTableName + " where gmt_create > ADDDATE(Date(?),INTERVAL 31 DAY)";
            selectContentSameAssert(sql, columnParam1, param);
        }
    }

    @Test
    public void TO_DAYSTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select TO_DAYS(gmt_create) as da from " + normaltblTableName + " where pk=1";

            rs = mysqlQueryData(sql, null);
            rc = andorQueryData(sql, null);

            String[] columnParam1 = { "da" };
            assertContentSame(rs, rc, columnParam1);
        }
    }

    @Test
    public void addtimeTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select ADDTIME(gmt_create,'1 1:1:1.000002')  as da from " + normaltblTableName
                         + " where pk=1";

            rs = mysqlQueryData(sql, null);
            rc = andorQueryData(sql, null);

            String[] columnParam1 = { "da" };
            assertContentSame(rs, rc, columnParam1);
        }
    }

    @Test
    public void dateTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select DATE(gmt_create)  as da from " + normaltblTableName + " where pk=1";

            rs = mysqlQueryData(sql, null);
            rc = andorQueryData(sql, null);

            String[] columnParam1 = { "da" };
            assertContentSame(rs, rc, columnParam1);
        }
    }

    /**
     * date函数通过绑定变量传值进去
     */
    @Test
    public void dateWithParamTest() throws Exception {
        if (!normaltblTableName.startsWith("ob")) {
            String sql = "select * from " + normaltblTableName + " where gmt_create >=Date(?)";
            List<Object> param = new ArrayList<Object>();
            param.add("2011-1-1");
            String[] columnParam = { "name", "pk", "id" };
            selectContentSameAssert(sql, columnParam, param);
        }
    }

    // @Test
    // public void str_to_dateTest() throws Exception {
    // String sql = "select * from " + normaltblTableName +
    // " where gmt_create between str_to_date(?,'%y-%m-%d')" +
    // " and str_to_date(?,'%y-%m-%d')";
    // List<Object> param =new ArrayList<Object>();
    // param.add("2001-1-1");
    // param.add("2013-1-1");
    // String [] columnParam={"name","pk","id"};
    // selectContentSameAssert(sql, columnParam, param);
    // }

    @Test
    public void nowTest() throws Exception {
        String sql = "select id,now() as dd from " + normaltblTableName + " where pk=1";
        rs = mysqlQueryData(sql, null);
        rc = andorQueryData(sql, null);

        String[] columnParam1 = { "dd", "id" };
        assertContentSame(rs, rc, columnParam1);

        sql = "select * from " + normaltblTableName + " where gmt_create = now()";
        rs = mysqlQueryData(sql, null);
        rc = andorQueryData(sql, null);
        String[] columnParam2 = { "gmt_create", "id" };
        assertContentSame(rs, rc, columnParam2);

        sql = "select * from " + normaltblTableName + " where now()>gmt_create";
        rs = mysqlQueryData(sql, null);
        rc = andorQueryData(sql, null);
        assertContentSame(rs, rc, columnParam2);

        sql = "select * from " + normaltblTableName + " where gmt_timestamp = now()";
        rs = mysqlQueryData(sql, null);
        rc = andorQueryData(sql, null);
        String[] columnParamTimestamp = { "gmt_timestamp", "id" };
        assertContentSame(rs, rc, columnParamTimestamp);

        sql = "select * from " + normaltblTableName + " where now()<=gmt_timestamp";
        rs = mysqlQueryData(sql, null);
        rc = andorQueryData(sql, null);
        assertContentSame(rs, rc, columnParamTimestamp);

        sql = "select * from " + normaltblTableName + " where gmt_datetime = now()";
        rs = mysqlQueryData(sql, null);
        rc = andorQueryData(sql, null);
        String[] columnParamDatetime = { "gmt_datetime", "id" };
        assertContentSame(rs, rc, columnParamDatetime);

        sql = "select * from " + normaltblTableName + " where now()>=gmt_datetime";
        rs = mysqlQueryData(sql, null);
        rc = andorQueryData(sql, null);
        assertContentSame(rs, rc, columnParamDatetime);

        if (!normaltblTableName.startsWith("ob")) {
            sql = "select * from " + normaltblTableName + " where gmt_create >( now()- INTERVAL ? day )";
            List<Object> param = new ArrayList<Object>();
            param.add(12);
            rs = mysqlQueryData(sql, param);
            rc = andorQueryData(sql, param);
            assertContentSame(rs, rc, columnParamDatetime);
        }
    }

}
