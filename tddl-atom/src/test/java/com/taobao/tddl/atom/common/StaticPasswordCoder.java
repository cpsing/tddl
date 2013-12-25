package com.taobao.tddl.atom.common;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import com.taobao.tddl.atom.securety.TPasswordCoder;
import com.taobao.tddl.common.utils.extension.Activate;

@Activate(order = 1)
public class StaticPasswordCoder implements TPasswordCoder {

    public String encode(String encKey, String secret) throws NoSuchAlgorithmException, NoSuchPaddingException,
                                                      InvalidKeyException, IllegalBlockSizeException,
                                                      BadPaddingException {
        if (secret.equals("change")) {
            return "change";
        } else {
            return "tddl";
        }
    }

    public String encode(String secret) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException,
                                       BadPaddingException, IllegalBlockSizeException {
        if (secret.equals("change")) {
            return "change";
        } else {
            return "tddl";
        }
    }

    public String decode(String encKey, String secret) throws NoSuchPaddingException, NoSuchAlgorithmException,
                                                      InvalidKeyException, BadPaddingException,
                                                      IllegalBlockSizeException {
        if (secret.equals("change")) {
            return "change";
        } else {
            return "tddl";
        }
    }

    public char[] decode(String secret) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException,
                                       BadPaddingException, IllegalBlockSizeException {
        if (secret.equals("change")) {
            return "change".toCharArray();
        } else {
            return "tddl".toCharArray();
        }
    }
}
