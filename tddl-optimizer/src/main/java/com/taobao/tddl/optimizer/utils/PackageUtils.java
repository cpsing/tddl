package com.taobao.tddl.optimizer.utils;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import com.google.common.collect.Lists;
import com.taobao.tddl.optimizer.exceptions.FunctionException;

/**
 * @author jianghang 2013-11-8 下午7:55:16
 * @since 5.0.0
 */
public class PackageUtils {

    public static List<Class> findClassesInPackage(String packageName, ClassFilter filter) {
        try {
            List<String> classes = Lists.newArrayList();
            String packageDirName = packageName.replace('.', '/');
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

            Enumeration<URL> dirEnumeration = classLoader.getResources(packageDirName);
            List<URL> dirs = Lists.newArrayList();
            while (dirEnumeration.hasMoreElements()) {
                URL dir = dirEnumeration.nextElement();
                dirs.add(dir);
            }

            Iterator<URL> dirIterator = dirs.iterator();
            while (dirIterator.hasNext()) {
                URL url = dirIterator.next();
                String protocol = url.getProtocol();
                if ("file".equals(protocol)) {
                    findClassesInDirPackage(packageName, URLDecoder.decode(url.getFile(), "UTF-8"), classes);
                } else if ("jar".equals(protocol)) {
                    JarFile jar = ((JarURLConnection) url.openConnection()).getJarFile();
                    Enumeration<JarEntry> entries = jar.entries();
                    while (entries.hasMoreElements()) {
                        JarEntry entry = entries.nextElement();
                        String name = entry.getName();
                        String pName = packageName;
                        if (name.charAt(0) == '/') {
                            name = name.substring(1);
                        }
                        if (name.startsWith(packageDirName)) {
                            int idx = name.lastIndexOf('/');
                            if (idx != -1) {
                                pName = name.substring(0, idx).replace('/', '.');
                            }

                            if (idx != -1) {
                                // it's not inside a deeper dir
                                if (name.endsWith(".class") && !entry.isDirectory()) {
                                    String className = name.substring(pName.length() + 1, name.length() - 6);
                                    classes.add(makeFullClassName(pName, className));
                                }
                            }
                        }
                    }
                }
            }

            List<Class> result = Lists.newArrayList();
            for (String clazz : classes) {
                if (filter == null || filter.preFilter(clazz)) {
                    Class<?> cls = null;
                    try {
                        cls = Class.forName(clazz);
                    } catch (Throwable e) {
                        // ignore
                    }

                    if (cls != null && (filter == null || filter.filter(cls))) {
                        result.add(cls);
                    }
                }
            }

            return result;
        } catch (IOException e) {
            throw new FunctionException("findClassesInPackage : " + packageName + " is failed. ", e);
        }

    }

    private static void findClassesInDirPackage(String packageName, String packagePath, List<String> classes) {
        File dir = new File(packagePath);
        if (!dir.exists() || !dir.isDirectory()) {
            return;
        }

        File[] dirfiles = dir.listFiles(new FileFilter() {

            @Override
            public boolean accept(File file) {
                return file.isDirectory() || (file.getName().endsWith(".class"));
            }
        });

        for (File file : dirfiles) {
            if (file.isDirectory()) {
                findClassesInDirPackage(makeFullClassName(packageName, file.getName()), file.getAbsolutePath(), classes);
            } else {
                String className = file.getName().substring(0, file.getName().lastIndexOf("."));
                classes.add(makeFullClassName(packageName, className));
            }
        }
    }

    private static String makeFullClassName(String pkg, String cls) {
        return pkg.length() > 0 ? pkg + "." + cls : cls;
    }

    public static interface ClassFilter {

        public boolean preFilter(String className);

        public boolean filter(Class clazz);
    }
}
