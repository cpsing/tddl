package com.taobao.tddl.rule.groovy;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 1.0
 * @since 1.6
 * @date 2011-8-17 07:29:21
 */
public class AppUtils {

    public static Long idFormat(Object value) {
        Long lva = Long.valueOf(String.valueOf(value));
        return lva * 10;
    }
}
