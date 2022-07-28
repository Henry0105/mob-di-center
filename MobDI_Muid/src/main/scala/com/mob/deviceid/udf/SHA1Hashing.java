package com.mob.deviceid.udf;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SHA1Hashing {

    public static String evaluate(String s) {
        if (s == null) {
            return null;
        } else if (s.length() == 0) {

            return s;
        } else {
            return shahash(s);
        }
    }

    public static String shahash(String input) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA1");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        md.update(input.getBytes());

        byte[] byteData = md.digest();

        //convert the byte to hex format method 1
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < byteData.length; i++) {
            sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
        }
        return sb.toString();
    }
}
