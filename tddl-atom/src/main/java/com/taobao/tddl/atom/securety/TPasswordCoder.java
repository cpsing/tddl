package com.taobao.tddl.atom.securety;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

public interface TPasswordCoder {

    public abstract String encode(String encKey, String secret) throws NoSuchAlgorithmException,
                                                               NoSuchPaddingException, InvalidKeyException,
                                                               IllegalBlockSizeException, BadPaddingException;

    public abstract String encode(String secret) throws NoSuchPaddingException, NoSuchAlgorithmException,
                                                InvalidKeyException, BadPaddingException, IllegalBlockSizeException;

    public abstract String decode(String encKey, String secret) throws NoSuchPaddingException,
                                                               NoSuchAlgorithmException, InvalidKeyException,
                                                               BadPaddingException, IllegalBlockSizeException;

    public abstract char[] decode(String secret) throws NoSuchPaddingException, NoSuchAlgorithmException,
                                                InvalidKeyException, BadPaddingException, IllegalBlockSizeException;

}
