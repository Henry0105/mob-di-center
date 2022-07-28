package com.mob.deviceid.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.util.regex.Pattern;

public class LuhnChecker {


    public static Boolean evaluate(String[] arguments) throws HiveException {
        if (null == arguments[0] || null == arguments[0]) {
            return false;
        }

        String imei =arguments[0].trim();
        if (!Pattern.matches("^[0-9A-Fa-f]{14,17}$", imei)) {
            return false;
        }

        if (14 == imei.length() || 16 == imei.length()) {
            return true;
        } else {
            // 有10进制和16进制两种计算方式
            if (Pattern.matches("^[0-9]{15,17}$", imei)) { // 10进制
                return genCode(imei.substring(0, 14), 10).equals(imei.substring(14,15));
            } else { // (Pattern.matches("^[0-9A-Fa-f]{14,17}$", imei)), ignore this pattern
                return true;
            }
        }
    }

    public static String genCode(String code, int base){
        int total=0,sum1=0,sum2 =0;
        int temp=0;
        char [] chs = code.toUpperCase().toCharArray();
        for (int i = 0; i < chs.length; i++) {
            int num = Integer.parseInt(String.valueOf(chs[i]), base);     // ascii to num

            /*(1)将奇数位数字相加(从1开始计数)*/
            if (i % 2 == 0) {
                sum1 = sum1 + num;
            }else{
                /*(2)将偶数位数字分别乘以2,分别计算个位数和十位数之和(从1开始计数)*/
                temp = num * 2 ;
                if (temp < base) {
                    sum2 = sum2 + temp;
                }else{
                    sum2 = sum2 + temp + 1 - base;
                }
            }
        }
        total = sum1 + sum2;
        /*如果得出的数个位是0则校验位为0,否则为base减去个位数 */
        if (total % base == 0) {
            return "0";
        }else{
            return (base - (total % base))+"";
        }
    }

}
