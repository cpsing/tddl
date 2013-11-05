package com.taobao.tddl.atom.securety.impl;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import com.taobao.tddl.atom.securety.TPasswordCoder;
import com.taobao.tddl.common.utils.extension.ExtensionLoader;

public class PasswordCoder implements TPasswordCoder {

    public TPasswordCoder delegate = null;

    public PasswordCoder(){
        delegate = ExtensionLoader.load(TPasswordCoder.class);
    }

    @Override
    public String encode(String encKey, String secret) throws NoSuchAlgorithmException, NoSuchPaddingException,
                                                      InvalidKeyException, IllegalBlockSizeException,
                                                      BadPaddingException {
        return delegate.encode(encKey, secret);
    }

    @Override
    public String encode(String secret) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException,
                                       BadPaddingException, IllegalBlockSizeException {
        return delegate.encode(secret);
    }

    @Override
    public String decode(String encKey, String secret) throws NoSuchPaddingException, NoSuchAlgorithmException,
                                                      InvalidKeyException, BadPaddingException,
                                                      IllegalBlockSizeException {
        return delegate.decode(encKey, secret);
    }

    @Override
    public char[] decode(String secret) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException,
                                       BadPaddingException, IllegalBlockSizeException {
        return delegate.decode(secret);
    }

}
