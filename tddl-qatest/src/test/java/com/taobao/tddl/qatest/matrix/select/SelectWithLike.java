package com.taobao.tddl.qatest.matrix.select;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.ExecuteTableName;
import com.taobao.tddl.qatest.util.EclipseParameterized;

/**
 * Comment for SelectWithLike
 * <p/>
 * Author By: zhuoxue.yll Created Date: 2012-7-9 下午12:39:15
 */
@RunWith(EclipseParameterized.class)
public class SelectWithLike extends BaseMatrixTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.normaltblTable(dbType));
    }

    public SelectWithLike(String tableName){
        BaseTestCase.normaltblTableName = tableName;
    }

    @Before
    public void prepareDate() throws Exception {
        normaltblPrepare(0, 20);
        normaltblTwoPrepare();
    }

    /**
     * like模糊匹配 '%'匹配符测试
     * 
     * @throws Exception
     */
    @Test
    public void LikeAnyTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where name like 'zhuo%'";
        String[] columnParam = { "ID", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select * from " + normaltblTableName + " where name like '%uo%'";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select * from " + normaltblTableName + " where name like '%uo%u%'";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * like模糊匹配 '_'匹配符测试
     * 
     * @throws Exception
     */
    @Test
    public void LikeOneTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where name like 'zhuoxu_'";
        String[] columnParam = { "ID", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select * from " + normaltblTableName + " where name like '_huoxu_'";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select * from " + normaltblTableName + " where name like '_hu_xu_'";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * like对特定字符测试，like匹配不区别大小写
     * 
     * @throws Exception
     */
    @Test
    public void LikeSpecificTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where name like 'zhuoxue'";
        String[] columnParam = { "ID", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        if (!normaltblTableName.contains("ob_")) {
            sql = "select * from " + normaltblTableName + " where name like 'ZHuoXUE'";
            selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
        }
    }

    /**
     * like对特定字符测试，like binary 区别大小写 暂时不支持
     * 
     * @throws Exception
     */
    @Ignore("like binary暂时不支持")
    @Test
    public void LikeBinaryTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where name like binary 'zhuoxue'";
        rs = mysqlQueryData(sql, null);
        rc = andorQueryData(sql, null);

        Assert.assertEquals(resultsSize(rs), resultsSize(rc));
    }

    /**
     * like对包含_和%匹配字段的匹配
     * 
     * @throws Exception
     */
    @Test
    public void MatchCharTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where name like 'zhuoxue\\_yll'";
        String[] columnParam = { "ID", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select * from " + normaltblTableName + " where name like 'zhuoxue\\%yll'";
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * NotLike测试，使用比较少，所以不支持
     */
    @Ignore("NotLike测试，使用比较少，所以不支持")
    @Test
    public void NotLikeTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where name not like 'zhuo%'";
        rs = mysqlQueryData(sql, null);
        rc = andorQueryData(sql, null);
        Assert.assertEquals(resultsSize(rs), resultsSize(rc));

        sql = "select * from " + normaltblTableName + " where name not like 'uo%'";
        rs = mysqlQueryData(sql, null);
        rc = andorQueryData(sql, null);
        Assert.assertEquals(resultsSize(rs), resultsSize(rc));

        sql = "select * from " + normaltblTableName + " where name not like 'zhuoxu_'";
        rs = mysqlQueryData(sql, null);
        rc = andorQueryData(sql, null);
        Assert.assertEquals(resultsSize(rs), resultsSize(rc));

        sql = "select * from " + normaltblTableName + " where name not like 'uoxu_'";
        rs = mysqlQueryData(sql, null);
        rc = andorQueryData(sql, null);
        Assert.assertEquals(resultsSize(rs), resultsSize(rc));

        sql = "select * from " + normaltblTableName + " where name not like 'ZHuoXUE'";
        rs = mysqlQueryData(sql, null);
        rc = andorQueryData(sql, null);
        Assert.assertEquals(resultsSize(rs), resultsSize(rc));
    }

    /**
     * like和其他字段一起的测试
     * 
     * @throws Exception
     */
    @Test
    public void likeWithAndTest() throws Exception {
        int id = 500;
        String sql = "select * from " + normaltblTableName + " where name like 'zhuoxue' and id>" + id;
        String[] columnParam = { "ID", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select * from " + normaltblTableName + " where name like 'zhuoxue\\%yll' and id <" + id;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void likeWithLimit() throws Exception {
        String sql = "select * from " + normaltblTableName + " where name like ? limit ?";
        List<Object> param = new ArrayList<Object>();
        param.add("zhuo%");
        param.add(10);
        selectConutAssert(sql, param);

        sql = "select * from " + normaltblTableName + " where name like ? limit ?,?";
        param.clear();
        param.add("%uo%");
        param.add(15);
        param.add(25);
        selectConutAssert(sql, param);

        sql = "select * from " + normaltblTableName + " as tab  where name like ? and gmt_timestamp > ? limit ?,?";
        param.clear();
        param.add("%uo%");
        param.add(gmt);
        param.add(15);
        param.add(25);
        selectConutAssert(sql, param);

        sql = "select * from " + normaltblTableName + " as tab where (name like ? and gmt_timestamp > ?) limit ?,?";
        selectConutAssert(sql, param);
    }

    @Test
    public void likeWithOrder() throws Exception {
        String[] columnParam = { "name", "gmt_timestamp" };
        String notKeyCloumn = "gmt_timestamp";
        String sql = "SELECT * from " + normaltblTableName
                     + " where name like ? and gmt_timestamp> ? and gmt_timestamp< ? order by gmt_timestamp";
        List<Object> param = new ArrayList<Object>();
        param.add("%zh%xue%");
        param.add(gmtBefore);
        param.add(gmtNext);
        selectOrderAssertNotKeyCloumn(sql, columnParam, param, notKeyCloumn);

        sql = "SELECT * from " + normaltblTableName
              + " where name like ? and gmt_timestamp> ? and gmt_timestamp< ? order by gmt_timestamp desc";
        selectOrderAssertNotKeyCloumn(sql, columnParam, param, notKeyCloumn);

        sql = "SELECT * from " + normaltblTableName
              + " as tab where (name like ? and (gmt_timestamp> ? and gmt_timestamp< ?)) order by  gmt_timestamp desc";
        selectOrderAssertNotKeyCloumn(sql, columnParam, param, notKeyCloumn);

        sql = "SELECT * from "
              + normaltblTableName
              + " as tab where (name like '%zhuo%' and (gmt_timestamp> '2011-1-1' and gmt_timestamp< '2018-7-9')) order by  gmt_timestamp desc limit 10";
        selectOrderAssertNotKeyCloumn(sql, columnParam, Collections.EMPTY_LIST, notKeyCloumn);

        sql = "SELECT * from "
              + normaltblTableName
              + " as tab where (name like ? and (gmt_timestamp> ? and gmt_timestamp< ?)) order by  gmt_timestamp desc limit ?,?";
        param.clear();
        param.add("%zh%xue%");
        param.add(gmtBefore);
        param.add(gmtNext);
        param.add(15);
        param.add(25);
        selectOrderAssertNotKeyCloumn(sql, columnParam, param, notKeyCloumn);
    }

}
