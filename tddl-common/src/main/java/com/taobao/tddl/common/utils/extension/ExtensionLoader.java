package com.taobao.tddl.common.utils.extension;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.commons.lang.exception.ExceptionUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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

    private static final String      SERVICES_DIRECTORY = "META-INF/services/";
    private static final String      TDDL_DIRECTORY     = "META-INF/tddl/";
    private static Map<Class, Class> providers          = Maps.newConcurrentMap();

    /**
     * 指定classloader加载server provider
     * 
     * @param service
     * @param loader
     * @return
     * @throws ExtensionNotFoundException
     */
    public static <S> S load(Class<S> service, ClassLoader loader) throws ExtensionNotFoundException {
        return loadFile(service, loader);
    }

    /**
     * 加载server provider
     * 
     * @param service
     * @return
     * @throws ExtensionNotFoundException
     */
    public static <S> S load(Class<S> service) throws ExtensionNotFoundException {
        return loadFile(service, findClassLoader());
    }

    /**
     * 获取所有的扩展类，按照{@linkplain Activate}定义的order顺序进行排序
     * 
     * @return
     */
    public static <S> List<Class> getAllExtendsionClass(Class<S> service) {
        return findAllExtensionClass(service, findClassLoader());
    }

    /**
     * 获取所有的扩展类，按照{@linkplain Activate}定义的order顺序进行排序
     * 
     * @return
     */
    public static <S> List<Class> getAllExtendsionClass(Class<S> service, ClassLoader loader) {
        return findAllExtensionClass(service, loader);
    }

    private static <S> S loadFile(Class<S> service, ClassLoader loader) {
        Class<?> extension = providers.get(service);
        if (extension == null) {
            synchronized (service) {
                extension = providers.get(service);
                if (extension == null) {
                    List<Class> extensions = findAllExtensionClass(service, loader);
                    if (extensions.isEmpty()) {
                        throw new ExtensionNotFoundException("not found service provider for : " + service.getName());
                    }
                    extension = extensions.get(extensions.size() - 1);// 最大的一个
                }
            }
        }

        try {
            return service.cast(extension.newInstance());
        } catch (InstantiationException e) {
            throw new ExtensionNotFoundException("not found service provider for : " + service.getName()
                                                 + " caused by " + ExceptionUtils.getFullStackTrace(e));
        } catch (IllegalAccessException e) {
            throw new ExtensionNotFoundException("not found service provider for : " + service.getName()
                                                 + " caused by " + ExceptionUtils.getFullStackTrace(e));
        }
    }

    private static <S> List<Class> findAllExtensionClass(Class<S> service, ClassLoader loader) {
        List<Class> extensions = Lists.newArrayList();
        try {
            loadFile(service, SERVICES_DIRECTORY, loader, extensions);
            loadFile(service, TDDL_DIRECTORY, loader, extensions);
        } catch (IOException e) {
            throw new ExtensionNotFoundException("not found service provider for : " + service.getName()
                                                 + " caused by " + ExceptionUtils.getFullStackTrace(e));
        }

        if (extensions.isEmpty()) {
            return extensions;
        }

        // 做一下排序
        Collections.sort(extensions, new Comparator<Class>() {

            public int compare(Class c1, Class c2) {
                Integer o1 = 0;
                Integer o2 = 0;
                Activate a1 = (Activate) c1.getAnnotation(Activate.class);
                Activate a2 = (Activate) c2.getAnnotation(Activate.class);

                if (a1 != null) {
                    o1 = a1.order();
                }

                if (a2 != null) {
                    o2 = a2.order();
                }

                return o1.compareTo(o2);

            }
        });
        return extensions;
    }

    private static void loadFile(Class<?> service, String dir, ClassLoader classLoader, List<Class> extensions)
                                                                                                               throws IOException {
        String fileName = dir + service.getName();
        Enumeration<java.net.URL> urls;
        if (classLoader != null) {
            urls = classLoader.getResources(fileName);
        } else {
            urls = ClassLoader.getSystemResources(fileName);
        }

        if (urls != null) {
            while (urls.hasMoreElements()) {
                java.net.URL url = urls.nextElement();
                try {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), "utf-8"));
                    String line = null;
                    while ((line = reader.readLine()) != null) {
                        final int ci = line.indexOf('#');
                        if (ci >= 0) {
                            line = line.substring(0, ci);
                        }
                        line = line.trim();
                        if (line.length() > 0) {
                            extensions.add(Class.forName(line, true, classLoader));
                        }
                    }
                } catch (Throwable e) {
                    // ignore
                }
            }
        }
    }

    private static ClassLoader findClassLoader() {
        return Thread.currentThread().getContextClassLoader();
    }

}
