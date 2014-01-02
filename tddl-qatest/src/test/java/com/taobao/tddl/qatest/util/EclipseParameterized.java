package com.taobao.tddl.qatest.util;

import java.lang.annotation.Annotation;

import org.junit.runner.Description;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runners.Parameterized;

public class EclipseParameterized extends Parameterized {

    public EclipseParameterized(Class<?> klass) throws Throwable{
        super(klass);
    }

    public void filter(Filter filter) throws NoTestsRemainException {
        super.filter(new FilterDecorator(filter));
    }

    /**
     * see http://youtrack.jetbrains.com/issue/IDEA-65966
     */
    private static class FilterDecorator extends Filter {

        private final Filter delegate;

        private FilterDecorator(Filter delegate){
            this.delegate = delegate;
        }

        @Override
        public boolean shouldRun(Description description) {
            return delegate.shouldRun(description) || delegate.shouldRun(wrap(description));
        }

        @Override
        public String describe() {
            return delegate.describe();
        }
    }

    private static Description wrap(Description description) {
        String name = description.getDisplayName();
        String fixedName = deparametrizedName(name);
        Description clonedDescription = Description.createSuiteDescription(fixedName, description.getAnnotations()
            .toArray(new Annotation[0]));
        for (Description child : description.getChildren()) {
            clonedDescription.addChild(wrap(child));
        }
        return clonedDescription;
    }

    private static String deparametrizedName(String name) {
        // Each parameter is named as [0], [1] etc
        if (name.startsWith("[")) {
            return name;
        }

        // Convert methodName[index](className) to
        // methodName(className)
        int indexOfOpenBracket = name.indexOf('[');
        int indexOfCloseBracket = name.indexOf(']') + 1;
        return name.substring(0, indexOfOpenBracket).concat(name.substring(indexOfCloseBracket));
    }
}
