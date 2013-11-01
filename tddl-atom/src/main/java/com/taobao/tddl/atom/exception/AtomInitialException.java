package com.taobao.tddl.atom.exception;

/**
 * @author qihao
 */
public class AtomInitialException extends Exception {

    private static final long serialVersionUID = -2933446568649742125L;

    public AtomInitialException(){
        super();
    }

    public AtomInitialException(String msg){
        super(msg);
    }

    public AtomInitialException(Throwable cause){
        super(cause);
    }

    public AtomInitialException(String msg, Throwable cause){
        super(msg, cause);
    }
}
