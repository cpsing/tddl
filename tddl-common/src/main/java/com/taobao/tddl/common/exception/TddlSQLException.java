package com.taobao.tddl.common.exception;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.SQLException;

import org.apache.commons.lang.exception.Nestable;
import org.apache.commons.lang.exception.NestableDelegate;

/**
 * Tddl nestabled {@link SQLException}
 * 
 * @author jianghang 2013-10-24 下午2:54:56
 * @since 5.0.0
 */
public class TddlSQLException extends SQLException implements Nestable {

    private static final long serialVersionUID = -4558269080286141706L;

    public TddlSQLException(SQLException cause){
        this(null, cause);
    }

    public TddlSQLException(String message, SQLException cause){
        super(message);
        if (cause == null) {
            throw new IllegalArgumentException("必须填入SQLException");
        }
        this.cause = cause;
    }

    protected NestableDelegate   delegate = new NestableDelegate(this);
    protected final SQLException cause;

    /**
     * {@inheritDoc}
     */
    public SQLException getCause() {
        return cause;
    }

    public String getMessage() {
        if (super.getMessage() != null) {
            return super.getMessage();
        } else if (cause != null) {
            return cause.toString();
        } else {
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    public String getMessage(int index) {
        if (index == 0) {
            return super.getMessage();
        }
        return delegate.getMessage(index);
    }

    /**
     * {@inheritDoc}
     */
    public String[] getMessages() {
        return delegate.getMessages();
    }

    /**
     * {@inheritDoc}
     */
    public Throwable getThrowable(int index) {
        return delegate.getThrowable(index);
    }

    /**
     * {@inheritDoc}
     */
    public int getThrowableCount() {
        return delegate.getThrowableCount();
    }

    /**
     * {@inheritDoc}
     */
    public Throwable[] getThrowables() {
        return delegate.getThrowables();
    }

    /**
     * {@inheritDoc}
     */
    public int indexOfThrowable(Class type) {
        return delegate.indexOfThrowable(type, 0);
    }

    /**
     * {@inheritDoc}
     */
    public int indexOfThrowable(Class type, int fromIndex) {
        return delegate.indexOfThrowable(type, fromIndex);
    }

    /**
     * {@inheritDoc}
     */
    public void printStackTrace() {
        delegate.printStackTrace();
    }

    /**
     * {@inheritDoc}
     */
    public void printStackTrace(PrintStream out) {
        delegate.printStackTrace(out);
    }

    /**
     * {@inheritDoc}
     */
    public void printStackTrace(PrintWriter out) {
        delegate.printStackTrace(out);
    }

    /**
     * {@inheritDoc}
     */
    public final void printPartialStackTrace(PrintWriter out) {
        super.printStackTrace(out);
    }

}
