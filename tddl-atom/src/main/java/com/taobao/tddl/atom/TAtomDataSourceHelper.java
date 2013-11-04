package com.taobao.tddl.atom;

import com.taobao.tddl.common.utils.thread.ThreadLocalMap;

/**
 * 提供给单独使用 TAtomDataSource 的用户指定应用连接限制的业务键 (Key) 以及其他执行信息。
 * 
 * @author changyuan.lh
 */
public class TAtomDataSourceHelper {

    /**
     * 指定应用连接限制的业务键 (Key)
     */
    public static final String CONN_RESTRICT_KEY = "CONN_RESTRICT_KEY";

    public static void setConnRestrictKey(Object key) {
        ThreadLocalMap.put(TAtomDataSourceHelper.CONN_RESTRICT_KEY, key);
    }

    public static Object getConnRestrictKey() {
        return ThreadLocalMap.get(TAtomDataSourceHelper.CONN_RESTRICT_KEY);
    }

    public static void removeConnRestrictKey() {
        ThreadLocalMap.remove(TAtomDataSourceHelper.CONN_RESTRICT_KEY);
    }
}
