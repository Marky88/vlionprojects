package com.advlion.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @description: 解析品牌用
 * @author: malichun
 * @time: 2021/5/25/0025 13:16
 */
public class ParseBandUDF extends UDF {

    public String evaluate(String producer) {
        if (producer == null) {
            return "unknown";
        }
        String originStr = producer.trim().toLowerCase();
        if (originStr.matches(".*huawei.*|.*-a[a-z]]\\d{2,3}|.*-tl\\d{2,3}|.*-tn\\d{2,3}|dig.*|pic.*|jer.*|sne.*|pra.*|ldn.*|are|fig.*|knt.*|ars.*|jny.*|rne.*|aqm.*|tah.*|art.*|spn.*|clt.*|jef.*|bac.*|ane.*|tas.*|lio.*|fla.*|lya.*|vog.*|pct.*||alp.*|pot.*|eml.*|mha.*|bla.*|sea.*|cdy.*|evr.*|els.*|eva.*|jsn.*|ele.*|par.*|bkl.*|pct.*|jkm.*|vtr.*|sea.*|ana.*|hma.*|vec.*|vky.*|wlz.*|hwi.*|was.*|stf.*|spn.*|ine.*|bnd.*|mar.*|vie.*|dub.*|lld.*|lon.*|glk.*|bln.*|duk.*|trt.*|vog*")) {
            return "huawei";
        } else if (originStr.matches("apple|.*iphone.*|ipad.*")) {
            return "apple";
        } else if (originStr.matches(".*oppo.*|p[a-z]{2,4}\\d{2,4}|r\\d[a-z].*")) {
            return "oppo";
        } else if (originStr.matches(".*vivo.*|v\\d{2,4}[a-z]{1,3}")) {
            return "vivo";
        } else if (originStr.matches(".*honor.*|yal.*|col.*|hlk.*|hry.*|bmh.*|oxf.*|ebg.*|tny.*|frd.*|cor.*|rvl.*|bkk.*|lra.*|jmm.*")) {
            return "honor";
        } else if (originStr.matches(".*xiaomi.*|^mi .*|mi\\d*.*|^mix .*|.*blackshark.*")) {
            return "xiaomi";
        } else if (originStr.matches("meizu|m\\d+ .*|16th.*|pro.*")) {
            return "meizu";
        } else if (originStr.matches("samsung.*|sm|sm-.*")) {
            return "samsung";
        } else if (originStr.matches(".*redmi.*")) {
            return "redmi";
        } else if (originStr.matches(".*oneplus.*|gm\\d+")) {
            return "oneplus";
        } else if (originStr.matches("gionee.*")) {
            return "gionee";
        } else if (originStr.matches("360.*")) {
            return "360";
        } else if (originStr.matches("meitu.*")) {
            return "meitu";
        } else if (originStr.matches("smartisan.*|os\\d+")) {
            return "smartisan";
        } else if (originStr.matches("nubia.*")) {
            return "nubia";
        } else if (originStr.matches(".*leeco.*|.*letv.*|.*lemobile.*|")) {
            return "leeco"; //乐视
        } else if (originStr.matches(".*lenovo.*")) {
            return "lenovo";
        } else if (originStr.matches("hisense.*")) {
            return "hisense";
        } else if (originStr.matches("zte.*")) {
            return "zte";
        } else if (originStr.matches("nokia.*")) {
            return "nokia";
        } else if (originStr.matches(".*coolpad.*")) {
            return "coolpad";
        } else if (originStr.matches("realme.*")) {
            return "realme";
        } else if (originStr.matches("motorola.*")) {
            return "motorola";
        } else if (originStr.matches("koobee.*")) {
            return "koobee";
        } else if (originStr.matches("sony.*")) {
            return "sony";
        } else if (originStr.matches("doov.*")) {
            return "doov";
        } else {
            return "unknown";
        }

    }

    public static void main(String[] args) {
        System.out.println();
    }

}
