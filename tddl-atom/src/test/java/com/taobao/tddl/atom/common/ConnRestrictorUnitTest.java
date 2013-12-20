package com.taobao.tddl.atom.common;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.atom.config.TAtomConfParser;
import com.taobao.tddl.atom.utils.AtomDataSourceHelper;
import com.taobao.tddl.atom.utils.ConnRestrictEntry;
import com.taobao.tddl.atom.utils.ConnRestrictSlot;
import com.taobao.tddl.atom.utils.ConnRestrictor;

public class ConnRestrictorUnitTest {

    @Test
    public void findSlot_查找连接槽() {
        String connRestrictStr = "K1,K4,K7:80%; K2,K5,K8:60%; K3,K6,K9:70%; *:4,50%; ~:20;";
        List<ConnRestrictEntry> connRestrictEntries = TAtomConfParser.parseConnRestrictEntries(connRestrictStr, 30);
        for (ConnRestrictEntry connRestrictEntry : connRestrictEntries) {
            System.out.println(connRestrictEntry.toString());
        }
        ConnRestrictor connRestrictor = new ConnRestrictor("TEST", connRestrictEntries);
        ConnRestrictSlot slotK1 = connRestrictor.findSlot("K1");
        ConnRestrictSlot slotK2 = connRestrictor.findSlot("K2");
        ConnRestrictSlot slotK3 = connRestrictor.findSlot("K3");
        ConnRestrictSlot slotK4 = connRestrictor.findSlot("K4");
        ConnRestrictSlot slotK5 = connRestrictor.findSlot("K5");
        ConnRestrictSlot slotK6 = connRestrictor.findSlot("K6");
        ConnRestrictSlot slotK7 = connRestrictor.findSlot("K7");
        ConnRestrictSlot slotK8 = connRestrictor.findSlot("K8");
        ConnRestrictSlot slotK9 = connRestrictor.findSlot("K9");
        ConnRestrictSlot slotK10 = connRestrictor.findSlot("K10");
        ConnRestrictSlot slotK11 = connRestrictor.findSlot("K11");
        ConnRestrictSlot slotNull = connRestrictor.findSlot(null);

        // 应该都有槽
        Assert.assertNotNull(slotK1);
        Assert.assertNotNull(slotK2);
        Assert.assertNotNull(slotK3);
        Assert.assertNotNull(slotK4);
        Assert.assertNotNull(slotK5);
        Assert.assertNotNull(slotK6);
        Assert.assertNotNull(slotK7);
        Assert.assertNotNull(slotK8);
        Assert.assertNotNull(slotK9);
        Assert.assertNotNull(slotK10);
        Assert.assertNotNull(slotK11);
        Assert.assertNotNull(slotNull);

        // 同一组 Key 应该分配到相同的槽
        Assert.assertSame(slotK1, slotK4);
        Assert.assertSame(slotK4, slotK7);
        Assert.assertSame(slotK2, slotK5);
        Assert.assertSame(slotK5, slotK8);
        Assert.assertSame(slotK3, slotK6);
        Assert.assertSame(slotK6, slotK9);

        // 每个 Key 的限制应该正确
        Assert.assertEquals(24, slotK7.getLimits());
        Assert.assertEquals(18, slotK8.getLimits());
        Assert.assertEquals(21, slotK9.getLimits());
        Assert.assertEquals(15, slotK10.getLimits());
        Assert.assertEquals(15, slotK11.getLimits());
        Assert.assertEquals(20, slotNull.getLimits());
    }

    @Test
    public void findSlot_HASH槽测试() {
        String connRestrictStr = "*:80%";
        List<ConnRestrictEntry> connRestrictEntries = TAtomConfParser.parseConnRestrictEntries(connRestrictStr, 30);
        for (ConnRestrictEntry connRestrictEntry : connRestrictEntries) {
            System.out.println(connRestrictEntry.toString());
        }
        ConnRestrictor connRestrictor = new ConnRestrictor("TEST", connRestrictEntries);
        ConnRestrictSlot slotK1 = connRestrictor.findSlot("K1");
        ConnRestrictSlot slotK2 = connRestrictor.findSlot("K2");
        ConnRestrictSlot slotK3 = connRestrictor.findSlot("K3");
        ConnRestrictSlot slotNull = connRestrictor.findSlot(null);
        Assert.assertNotNull(slotK1);
        Assert.assertNotNull(slotK2);
        Assert.assertNotNull(slotK3);
        Assert.assertNull(slotNull);

        Assert.assertSame(slotK1, slotK2);
        Assert.assertSame(slotK2, slotK3);
        Assert.assertEquals(24, slotK1.getLimits());
    }

    @Test
    public void doRestrict_连接限制() {
        String connRestrictStr = "K1,K4,K7:80%; K2,K5,K8:60%; K3,K6,K9:70%; *:4,50%; ~:20;";
        List<ConnRestrictEntry> connRestrictEntries = TAtomConfParser.parseConnRestrictEntries(connRestrictStr, 30);
        ConnRestrictor connRestrictor = new ConnRestrictor("TEST", connRestrictEntries);
        List<ConnRestrictSlot> slotList = new ArrayList<ConnRestrictSlot>();
        int tries = 0;
        try {
            // K1 的分配检查
            AtomDataSourceHelper.setConnRestrictKey("K1");
            for (tries = 0; tries < 30; tries++) {
                slotList.add(connRestrictor.doRestrict(1));
            }
        } catch (SQLException e) {
            // 分配异常检查
            Assert.assertEquals(24, tries);
        } finally {
            // 分配数目检查
            Assert.assertEquals(24, slotList.size());
            AtomDataSourceHelper.removeConnRestrictKey();
            for (ConnRestrictSlot connRestrictSlot : slotList) {
                connRestrictSlot.freeConnection();
            }
            slotList.clear();
        }

        try {
            // K5 的分配检查
            AtomDataSourceHelper.setConnRestrictKey("K5");
            for (tries = 0; tries < 30; tries++) {
                slotList.add(connRestrictor.doRestrict(1));
            }
        } catch (SQLException e) {
            // 分配异常检查
            Assert.assertEquals(18, tries);
        } finally {
            // 分配数目检查
            Assert.assertEquals(18, slotList.size());
            AtomDataSourceHelper.removeConnRestrictKey();
            for (ConnRestrictSlot connRestrictSlot : slotList) {
                connRestrictSlot.freeConnection();
            }
            slotList.clear();
        }

        try {
            // K9 的分配检查
            AtomDataSourceHelper.setConnRestrictKey("K9");
            for (tries = 0; tries < 30; tries++) {
                slotList.add(connRestrictor.doRestrict(1));
            }
        } catch (SQLException e) {
            // 分配异常检查
            Assert.assertEquals(21, tries);
        } finally {
            // 分配数目检查
            Assert.assertEquals(21, slotList.size());
            AtomDataSourceHelper.removeConnRestrictKey();
            for (ConnRestrictSlot connRestrictSlot : slotList) {
                connRestrictSlot.freeConnection();
            }
            slotList.clear();
        }

        try {
            // HASH 方式 K12 的分配检查
            AtomDataSourceHelper.setConnRestrictKey("K12");
            for (tries = 0; tries < 30; tries++) {
                slotList.add(connRestrictor.doRestrict(1));
            }
        } catch (SQLException e) {
            // 分配异常检查
            Assert.assertEquals(15, tries);
        } finally {
            // 分配数目检查
            Assert.assertEquals(15, slotList.size());
            AtomDataSourceHelper.removeConnRestrictKey();
            for (ConnRestrictSlot connRestrictSlot : slotList) {
                connRestrictSlot.freeConnection();
            }
            slotList.clear();
        }

        try {
            // null Key 的分配检查
            AtomDataSourceHelper.removeConnRestrictKey();
            for (tries = 0; tries < 20; tries++) {
                slotList.add(connRestrictor.doRestrict(1));
            }
        } catch (SQLException e) {
            // 分配异常检查
            Assert.assertEquals(20, tries);
        } finally {
            // 分配数目检查
            Assert.assertEquals(20, slotList.size());
            for (ConnRestrictSlot connRestrictSlot : slotList) {
                connRestrictSlot.freeConnection();
            }
            slotList.clear();
        }
    }
}
