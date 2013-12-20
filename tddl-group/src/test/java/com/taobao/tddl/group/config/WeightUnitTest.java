package com.taobao.tddl.group.config;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author yangzhu
 */
public class WeightUnitTest {

    @Test
    public void all() {
        Weight w = new Weight(null);
        Assert.assertEquals(w.r, 10);
        Assert.assertEquals(w.w, 10);
        Assert.assertEquals(w.p, 0);
        Assert.assertEquals(w.q, 0);

        w = new Weight("");
        Assert.assertEquals(w.r, 0);
        Assert.assertEquals(w.w, 0);
        Assert.assertEquals(w.p, 0);
        Assert.assertEquals(w.q, 0);

        w = new Weight("   ");
        Assert.assertEquals(w.r, 0);
        Assert.assertEquals(w.w, 0);
        Assert.assertEquals(w.p, 0);
        Assert.assertEquals(w.q, 0);

        w = new Weight("rwpq");
        Assert.assertEquals(w.r, 10);
        Assert.assertEquals(w.w, 10);
        Assert.assertEquals(w.p, 0);
        Assert.assertEquals(w.q, 0);

        w = new Weight("");
        Assert.assertEquals(w.r, 0);
        Assert.assertEquals(w.w, 0);
        Assert.assertEquals(w.p, 0);
        Assert.assertEquals(w.q, 0);

        w = new Weight("r10w20p1q2");
        Assert.assertEquals(w.r, 10);
        Assert.assertEquals(w.w, 20);
        Assert.assertEquals(w.p, 1);
        Assert.assertEquals(w.q, 2);

        w = new Weight("R10W20P1Q2");
        Assert.assertEquals(w.r, 10);
        Assert.assertEquals(w.w, 20);
        Assert.assertEquals(w.p, 1);
        Assert.assertEquals(w.q, 2);
    }
}
