package com.taobao.tddl.optimizer.utils;

import java.util.List;

import org.junit.Test;

import com.taobao.tddl.optimizer.core.expression.IFunction;

/**
 * @author jianghang 2013-11-8 下午8:05:42
 * @since 5.0.0
 */
public class PackageUtilsTest {

    @Test
    public void testFile() {
        List<Class> classes = PackageUtils.findClassesInPackage(IFunction.class.getPackage().getName(), null);
        for (Class clazz : classes) {
            System.out.println(clazz);
        }
    }

    @Test
    public void testJar() {
        List<Class> classes = PackageUtils.findClassesInPackage("com.google.common.annotations", null);
        for (Class clazz : classes) {
            System.out.println(clazz);
        }
    }
}
