package com.taobao.tddl.qatest.group;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

/**
 * 对Group层的hint测试
 * 
 * @author yaolingling.pt
 */
public class GroupGroupIndexTest extends GroupTestCase {

    /**
     * 对GroupIndex的hint测试
     */
    @Test
    public void GroupIndexHintTest() {

        String sql = "insert into normaltbl_0001 (pk,gmt_create) values (?,?)";
        Object[] arguments = new Object[] { RANDOM_ID, time };
        tddlJT.update(sql, arguments);

        // 验证在0库中能查找到数据
        sql = "/*+TDDL_GROUP({groupIndex:0})*/select * from normaltbl_0001 where pk = ?";
        Map re = tddlJT.queryForMap(sql, new Object[] { RANDOM_ID });
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));

        // 在1库中找不到对应的数据
        sql = "/*+TDDL_GROUP({groupIndex:1})*/select * from normaltbl_0001 where pk = ?";
        List ls = (List) tddlJT.queryForList(sql, new Object[] { RANDOM_ID });
        Assert.assertEquals(0, ls.size());

    }

}
