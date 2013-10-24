package com.taobao.tddl.common.utils;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

/**
 * 各种不同实现的比较测试
 * 
 * @author linxuan
 */
public class TStringUtilTest {

    @Test
    public void testGetBetween() {
        Assert.assertEquals(TStringUtil.getBetween("wx[ b ]yz", "[", "]"), "b");
        Assert.assertEquals(TStringUtil.getBetween(null, "a", "a"), null);
        Assert.assertEquals(TStringUtil.getBetween("abc", null, "a"), null);
        Assert.assertEquals(TStringUtil.getBetween("abc", "a", null), null);
        Assert.assertEquals(TStringUtil.getBetween("", "", ""), "");
        Assert.assertEquals(TStringUtil.getBetween("", "", "]"), null);
        Assert.assertEquals(TStringUtil.getBetween("", "[", "]"), null);
        Assert.assertEquals(TStringUtil.getBetween("yabcz", "", ""), "");
        Assert.assertEquals(TStringUtil.getBetween("yabcz", "y", "z"), "abc");
        Assert.assertEquals(TStringUtil.getBetween("yabczyabcz", "y", "z"), "abc");
    }

    @Test
    public void testRemoveBetween() {
        Assert.assertEquals(TStringUtil.removeBetween("abc[xxx]bc", "[", "]"), "abc bc");
    }

    @Test
    public void testTwoPartSplit() {
        Assert.assertArrayEquals(TStringUtil.twoPartSplit("abc:bc:bc", ":"), new String[] { "abc", "bc:bc" });
        Assert.assertArrayEquals(TStringUtil.twoPartSplit(null, "a"), new String[] { null });
        Assert.assertArrayEquals(TStringUtil.twoPartSplit("abc:bc", "d"), new String[] { "abc:bc" });
        Assert.assertArrayEquals(TStringUtil.twoPartSplit("abc:bc", ";"), new String[] { "abc:bc" });
    }

    @Test
    public void testRecursiveSplit() {
        Assert.assertEquals(TStringUtil.recursiveSplit("abc:bc:bc", ":"), Arrays.asList("abc", "bc", "bc"));
        Assert.assertEquals(TStringUtil.recursiveSplit("abc:bc", "d"), Arrays.asList("abc:bc"));
        Assert.assertEquals(TStringUtil.recursiveSplit("abc:bc", ";"), Arrays.asList("abc:bc"));
    }

    @Test
    public void testFillTabWithSpace() {
        String sql = "   select sum(rate)      from                                                                          feed_receive_0117                                                            t             where       RATED_UID=?     and RATER_UID=?     and suspended=0 and validscore=1      and rater_type=?     and trade_closingdate>=?     and trade_closingdate<?     and id<>?        and (IFNULL(IMPORT_FROM, 0)&8) = 0        #@#mysql_feel_01#@#EXECUTE_A_SQL_TIMEOUT#@#1#@#484#@#484#@#484";
        String assertSql = "select sum(rate) from feed_receive_0117 t where RATED_UID=? and RATER_UID=? and suspended=0 and validscore=1 and rater_type=? and trade_closingdate>=? and trade_closingdate<? and id<>? and (IFNULL(IMPORT_FROM, 0)&8) = 0 #@#mysql_feel_01#@#EXECUTE_A_SQL_TIMEOUT#@#1#@#484#@#484#@#484";
        String acutulSql = null;
        acutulSql = TStringUtil.fillTabWithSpace(sql);
        Assert.assertEquals(assertSql, acutulSql);
    }

    @Test
    public void testRemoveBetweenWithSplitor() {
        String sql = "/*+UNIT_VALID({valid_key:123})*/select * from table";
        sql = TStringUtil.removeBetween(sql, "/*+UNIT_VALID(", ")*/");
        Assert.assertEquals(" select * from table", sql);
    }
}
