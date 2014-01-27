package com.taobao.tddl.common.utils.thread;

import java.util.HashMap;
import java.util.Map;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * @author jianghang 2013-10-24 下午2:01:31
 * @since 5.0.0
 */
public class ThreadLocalMap {

    private static final Logger                             logger           = LoggerFactory.getLogger(ThreadLocalMap.class);
    protected final static ThreadLocal<Map<Object, Object>> threadContext = new MapThreadLocal();

    public static void put(Object key, Object value) {
        getContextMap().put(key, value);
    }

    public static Object remove(Object key) {
        return getContextMap().remove(key);
    }

    public static Object get(Object key) {
        return getContextMap().get(key);
    }

    public static boolean containsKey(Object key) {
        return getContextMap().containsKey(key);
    }

    private static class MapThreadLocal extends ThreadLocal<Map<Object, Object>> {

        protected Map<Object, Object> initialValue() {
            return new HashMap<Object, Object>() {

                private static final long serialVersionUID = 3637958959138295593L;

                public Object put(Object key, Object value) {
                    if (logger.isDebugEnabled()) {
                        if (containsKey(key)) {
                            logger.debug("Overwritten attribute to thread context: " + key + " = " + value);
                        } else {
                            logger.debug("Added attribute to thread context: " + key + " = " + value);
                        }
                    }

                    return super.put(key, value);
                }
            };
        }
    }

    /**
     * 取得thread context Map的实例。
     * 
     * @return thread context Map的实例
     */
    protected static Map<Object, Object> getContextMap() {
        return (Map<Object, Object>) threadContext.get();
    }

    /**
     * 清理线程所有被hold住的对象。以便重用！
     */

    public static void reset() {
        getContextMap().clear();
    }
}
