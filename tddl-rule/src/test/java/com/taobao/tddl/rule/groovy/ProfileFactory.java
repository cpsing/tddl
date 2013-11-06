package com.taobao.tddl.rule.groovy;

/**
 * @description
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 1.0
 * @since 1.6
 * @date 2011-8-17 07:23:43
 */
public class ProfileFactory {

    private static SimpleProfile profile = new SimpleProfile();

    public static SimpleProfile getProfile() {
        return profile;
    }
}
