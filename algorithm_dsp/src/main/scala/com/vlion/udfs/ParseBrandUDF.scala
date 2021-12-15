package com.vlion.udfs

/**
 * @description:
 * @author:
 * @time:
 *
 */
class ParseBrandUDF extends Serializable {

    def parseBrand(producer:String):String={
        if(producer == null){
            return null
        }
        val originStr=producer.trim.toLowerCase
        originStr match {
            case x if
            x.matches(".*huawei.*|.*-a[a-z]]\\d{2,3}|.*-tl\\d{2,3}|.*-tn\\d{2,3}|fig.*|vog.*|dig.*|pic.*|jer.*|sne.*|pra.*|ldn.*|are|fig.*|knt.*|ars.*|jny.*|rne.*|aqm.*|tah.*|art.*|spn.*|clt.*|jef.*|bac.*|ane.*|tas.*|lio.*|fla.*|lya.*|vog.*|pct.*||alp.*|pot.*|eml.*|mha.*|bla.*|sea.*|cdy.*|evr.*|els.*|eva.*|jsn.*|ele.*|par.*|bkl.*|pct.*|jkm.*|vtr.*|sea.*|ana.*|hma.*|vec.*|vky.*|wlz.*|hwi.*|was.*|stf.*|spn.*|ine.*|bnd.*|mar.*|vie.*|dub.*|lld.*|lon.*|glk.*|bln.*|duk.*|trt.*|vog*")
            =>  "huawei"   //华为
            case x if x matches "apple|.*iphone.*|ipad.*" => "apple"    //苹果
            case x if x.matches(".*oppo.*|p[a-z]{2,4}\\d{2,4}|r\\d[a-z].*") => "oppo"   //oppo
            case x if x.matches(".*vivo.*|v\\d{2,4}[a-z]{1,3}") => "vivo"  //vivo
            case x if x.matches(".*honor.*|yal.*|col.*|hlk.*|hry.*|bmh.*|oxf.*|ebg.*|tny.*|frd.*|cor.*|rvl.*|bkk.*|lra.*|jmm.*")
            => "honor"   //荣耀
            case x if x.matches(".*xiaomi.*|^mi .*|mi\\d*.*|^mix .*|.*blackshark.*")
            => "xiaomi"                                  //小米
            case x if x matches "meizu|m\\d+ .*|16th.*|pro.*" => "meizu"   //魅族
            case x if x matches "samsung.*|sm|sm-.*" => "samsung"   //三星
            case  x if x.matches(".*redmi.*") => "redmi"       //红米
            case x if x.matches(".*oneplus.*|gm\\d+") => "oneplus"  //一加
            case x if x.matches("gionee.*") => "gionee"    //金立
            case x if x.matches("360.*") => "360"           //360
            case x if x matches "meitu.*" => "meitu"       //美图
            case x if x matches "smartisan.*|os\\d+" => "smartisan"  //锤子
            case x if x.matches("nubia.*") => "nubia"            //努比亚
            case x if x.matches(".*leeco.*|.*letv.*|.*lemobile.*|") => "leeco"  //乐视
            case x if x.matches(".*lenovo.*") => "lenovo" //联想
            case x if x.matches("hisense.*") => "hisense"  //海信
            case x if x.matches("zte.*") => "zte"   //中兴
            case x if x.matches("nokia.*") => "nokia"    //诺基亚
            case x if x.matches(".*coolpad.*") => "coolpad"  //酷派
            case x if x matches "realme.*" => "realme"     // realme
            case x if x matches "motorola.*" => "motorola"  //摩托罗拉
            case x if x matches "koobee.*" => "koobee"    //酷比
            case x if x matches "sony.*" => "sony"        //索尼
            case x if x matches "doov.*" => "doov"       //朵唯


            //            case x if x matches "xiaolajiao" => "xiaolajiao"
            //            case x if x matches "sugar" => "sugar"

            case _ => "other"

        }



    }

}
