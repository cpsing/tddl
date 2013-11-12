package com.taobao.tddl.group.config;

import java.util.Arrays;

import org.junit.Test;

public class GroupConfigHolderTest {

    @Test
    public void test1() {
        GroupConfigHolder holder = new GroupConfigHolder("JIECHEN_YUGONG_APP", Arrays.asList("YUGONG_TEST_APP_GROUP_1",
            "YUGONG_TEST_APP_GROUP_2"), null);
        holder.init();
        System.out.println("OUT");
    }

}
