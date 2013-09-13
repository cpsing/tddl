package com.taobao.tddl.common.extension;

import java.util.ServiceLoader;

/**
 * 包装{@linkplain ServiceLoader}，提供SPI的获取方式 <br/>
 * jdk自带的{@linkplain ServiceLoader}
 * 存在一些局限，无法为SPI扩展定义排序规则，如果classpath存在多个SPI配置时，加载无法确定，所以包装一下允许以后替换实现
 * 
 * @see <a
 * href="http://java.sun.com/j2se/1.5.0/docs/guide/jar/jar.html#Service%20Provider">JDK5.0的自动发现机制实现</a>
 * @author jianghang 2013-9-13 下午3:38:30
 * @since 5.1.0
 */
public class ExtensionLoader<S> {

    /**
     * 指定classloader加载server provider
     * 
     * @param service
     * @param loader
     * @return
     * @throws ExtensionNotFoundException
     */
    public static <S> S load(Class<S> service, ClassLoader loader) throws ExtensionNotFoundException {
        ServiceLoader<S> result = ServiceLoader.load(service, loader);
        for (S clazz : result) {
            return clazz;
        }

        throw new ExtensionNotFoundException("not found service provider for : " + service.getName());
    }

    /**
     * 加载server provider
     * 
     * @param service
     * @return
     * @throws ExtensionNotFoundException
     */
    public static <S> S load(Class<S> service) throws ExtensionNotFoundException {
        ServiceLoader<S> result = ServiceLoader.load(service);
        for (S clazz : result) {
            return clazz;
        }

        throw new ExtensionNotFoundException("not found service provider for : " + service.getName());
    }
}
