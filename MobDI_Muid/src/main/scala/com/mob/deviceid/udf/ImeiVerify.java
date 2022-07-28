package com.mob.deviceid.udf;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ImeiVerify {

    private static final String IMEI_REGEX = "^[0-9]*$";
    private static final String MEID_REGEX = "^[0-9a-f]*$";
    private static Pattern regex = Pattern.compile(IMEI_REGEX);
    private static Pattern meidRegex = Pattern.compile(MEID_REGEX);

    public static boolean evaluate(String str) {
        boolean result = false;
        if (str == null) {
            return result;
        }
        String imei = str.trim().replaceAll(" |/|-", "").toLowerCase();
        if (imei.length() >= 14 && imei.length() <= 17 && imei.length() != 16) {
            Matcher matcher = regex.matcher(imei);
            // 判断是否为纯数字的imei
            if (matcher.matches()) {
                if (imei.length() == 14) {
                    result = true;
                } else {
                    result = check(imei);
                }
            } else {
                // 不是纯数字imei要过滤长度大于14的和非16进制的
                if (imei.length() < 15) {
                    Matcher meidMatcher = meidRegex.matcher(imei);
                    if (meidMatcher.matches()) {
                        result = true;
                    }
                }

            }
        }
        return result;
    }

    private static boolean check(String ccNumber) {
        int sum = 0;
        boolean alternate = true;
        for (int i = 13; i >= 0; i--) {
            int n = Integer.parseInt(ccNumber.substring(i, i + 1), 16);
            if (alternate) {
                n *= 2;
                if (n > 9) {
                    n = (n % 10) + 1;
                }
            }
            sum += n;
            alternate = !alternate;
        }
        int result = Integer.parseInt(ccNumber.substring(14, 15), 16);
        return (sum + result) % 10 == 0;
    }
}
