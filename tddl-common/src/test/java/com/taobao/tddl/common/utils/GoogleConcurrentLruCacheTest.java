package com.taobao.tddl.common.utils;

import org.junit.Test;

import com.googlecode.concurrentlinkedhashmap.EvictionListener;

public class GoogleConcurrentLruCacheTest {

    @Test
    public void testSimple() {

        GoogleConcurrentLruCache cache = new GoogleConcurrentLruCache(10, new EvictionListener<String, String>() {

            public void onEviction(String key, String value) {
                System.out.println("evict key:" + key + " values:" + value);
            }
        });

        for (int i = 0; i < 11; i++) {
            cache.put("key" + i, "value" + i);
        }
    }
}
