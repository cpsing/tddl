package com.taobao.tddl.group.utils;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.group.config.GroupIndex;

public class GroupHintParserTest {

    @Test
    public void testSingleIndex() {
        String sql = "/*+TDDL_GROUP({groupIndex:12})*/select * from tab";
        GroupIndex index = GroupHintParser.convertHint2Index(sql);
        Assert.assertEquals(index.index, 12);
        Assert.assertEquals(index.failRetry, false);
    }

    @Test
    public void testIndexWithRetry() {
        String sql = "/*+TDDL_GROUP({groupIndex:12,failRetry:true})*/select * from tab";
        GroupIndex index = GroupHintParser.convertHint2Index(sql);
        Assert.assertEquals(index.index, 12);
        Assert.assertEquals(index.failRetry, true);
    }

    @Test
    public void testNoIndexHint() {
        String sql = "select * from tab";
        GroupIndex index = GroupHintParser.convertHint2Index(sql);
        Assert.assertNull(index);
    }

    @Test
    public void testRemoveIndex() {
        String sql = "/*+TDDL_GROUP({groupIndex:12,failRetry:true})*/select * from tab";
        String remove = GroupHintParser.removeTddlGroupHint(sql);
        Assert.assertEquals(remove, " select * from tab");
    }

    @Test
    public void testExceptionOnlyRetry() {
        String sql = "/*+TDDL_GROUP({failRetry:true})*/select * from tab";
        try {
            GroupHintParser.convertHint2Index(sql);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("the standard group hint is:'groupIndex:12[,failRetry:true]',current index hint is:failRetry:true",
                e.getMessage());
        }
    }

    @Test
    public void testExceptionErrorHint() {
        String sql = "/*+TDDL_GROUP({groupIndexx:12})*/select * from tab";
        try {
            GroupHintParser.convertHint2Index(sql);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("the standard group hint is:'groupIndex:12[,failRetry:true]',current index hint is:groupIndexx:12",
                e.getMessage());
        }
    }
}
