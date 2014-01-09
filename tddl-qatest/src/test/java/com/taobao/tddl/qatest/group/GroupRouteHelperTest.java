package com.taobao.tddl.qatest.group;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.common.GroupDataSourceRouteHelper;

/**
 * Comment for GroupRouteHelperIntegrationTest
 * <p/>
 * Created Date: 2010-12-8 下午06:55:29
 */
@SuppressWarnings("rawtypes")
public class GroupRouteHelperTest extends GroupTestCase {

    @Test
    public void executeByGroupDataSourceIndexTest() {
        String sql = "insert into normaltbl_0001 (pk,gmt_create) values (?,?)";
        Object[] arguments = new Object[] { RANDOM_ID, time };
        tddlJT.update(sql, arguments);

        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
        Map re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));

        // 因为没有库复制数据，所以另外两个库上肯定没有数据（干扰数据除外）
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
        List rsList = tddlJT.queryForList("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(0, rsList.size());
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(2);
        rsList = tddlJT.queryForList("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(0, rsList.size());

        /*
         * 清除数据，虽然Group中qatest_normal_0_bac为只读，
         * 但如果指定数据库的情况下，只要对应的AtomDS可写，将可以正常写入
         */
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
        clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });

        // 同上
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
        String sqlx = "insert into normaltbl_0001 (pk,gmt_create) values (?,?)";
        tddlJT.update(sqlx, new Object[] { RANDOM_ID, time });

        // 验证插入数据
        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
        re = tddlJT.queryForMap("select * from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
        Assert.assertEquals(time, String.valueOf(re.get("gmt_create")));

        GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(1);
        clearData(tddlJT, "delete from normaltbl_0001 where pk=?", new Object[] { RANDOM_ID });
    }
}
