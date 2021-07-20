package com.vlion.service.imp;

import com.vlion.dao.ExcelDao;
import com.vlion.dao.imp.ExcelDaoImp;
import com.vlion.service.ExcelService;
import com.vlion.utils.Utils;
import org.apache.poi.ss.usermodel.Workbook;
import org.javatuples.Pair;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @description:
 * @author: malichun
 * @time: 2021/3/22/0022 15:47
 */
public class ExcelServiceImp implements ExcelService {
    ExcelDao zd = new ExcelDaoImp();

    @Override
    public Workbook parseZhifubaoExcelService(InputStream is, String fileName) throws IOException {


        Map<String, List<Object>> excelListMap = zd.parseZfbInputExcel(is, fileName); //key是 date_channel

        List<String> etlDateList = excelListMap.remove("etlDateSet").stream().map(Object::toString).collect(Collectors.toList()); //拿到处理时间

        List<List<Object>> res1 = zd.QuerySQL1(); //所有的计划id,name,media_id,adsolcation_id
        Map<String, Map<String, List<Object>>> map = getAllPlanNameIDMap(res1);
        Map<String, List<Object>> allPlanNameIDMap = map.get("allPlanNameID"); // 计划name, list
        Map<String, List<Object>> allAdslocateIDMap = map.get("allPlanIDAdslocateID");  //广告位id, list

        //查找出所有的匹配到的计划plan_id //找出广告位id,为sql3准备
        List<Pair<String, Object>> innerPlanIdAdsolcateId = excelListMap.entrySet().stream().map(m -> {
            String key = m.getKey();// 日期_channel
            List<Object> value = m.getValue();  //整个ArrayList
            List<Object> query1Res = allPlanNameIDMap.get(key.substring(key.indexOf("_") + 1));
            if (query1Res != null && query1Res.get(0) != null) {
                return Pair.with(query1Res.get(0).toString(), query1Res.get(3)); //返回一个元组(plainid,adslotId)
            }
            return null;
        }).filter(Objects::nonNull).distinct().collect(Collectors.toList());

        //找出匹配的计划id
        List<String> innerPlanIds = innerPlanIdAdsolcateId.stream()
            .map(Pair::getValue0)
            .filter(Objects::nonNull)
            .distinct()
            .collect(Collectors.toList());

        //查找sql2,计划id
        List<List<Object>> query2Res = zd.QuerySQL2(etlDateList, innerPlanIds);
        //找出匹配的广告位id
        List<String> innerAdsolcateId = innerPlanIdAdsolcateId
            .stream()
            .map(pair -> {
                if (pair.getValue1() != null) {
                    return pair.getValue1().toString();
                }
                return null;
            }).filter(Objects::nonNull).distinct().collect(Collectors.toList());

        //查找广告位id
        List<List<Object>> query3Res = zd.QuerySQL3(etlDateList, innerAdsolcateId);

        //query2,query3的map
        Map<String, List<Object>> planId2Map = getTokenMap(query2Res);
        Map<String, List<Object>> adsoltId3Map = getTokenMap(query3Res);

        //查询4
        // 日期_计划id , 支付宝新登, 支付宝拉新, 支付宝mau
        List<List<Object>> query4Res = zd.querySQL4(etlDateList, innerPlanIds); //token          |aduser_id|alp1|alp2 |alp4|
        Map<String, List<Object>> planId4Map = getTokenMap(query4Res); // key: 日期_计划id


        // 查询8,获取真实的ecpm
        List<List<Object>> query8Res = zd.querySql8(etlDateList, innerPlanIds);
        Map<String, List<Object>> planId8Map = getTokenMap(query8Res);

        //开始组装
        List<List<Object>> generatedList = excelListMap.entrySet().stream().map(e -> {
            String key = e.getKey();  //date_channel
            List<Object> value = e.getValue(); // 一行所有
            String[] arr = key.split("_");
            String date1 = arr[0];
            String col0 = Utils.convertDateStr(date1); //日期
            String col1 = arr[1]; //名称,渠道号
            List<Object> planNameObjects = allPlanNameIDMap.get(col1);
            if (planNameObjects == null) {
                return null;
            }

            Object col2 = planNameObjects.get(2); //媒体
            String planId = Utils.getObjectValueString(planNameObjects.get(0));

            //sql的key
            String datePlanIdKey = col0 + "_" + planId;

            String col3 = planId + " / " + Utils.getObjectValueString(planNameObjects.get(1)); // 计划
            List<Object> planIdValueList = planId2Map.get(datePlanIdKey);

            int col4 = 0;
            int col5 = 0;
            double col6 = 0.0;
            double col7 = 0.0;
            double col8 = 0.0;
            double col9 = 0.0;
            if (planIdValueList != null) {
                col4 = Utils.getObjectValueInteger(planIdValueList.get(3)); // 曝光
                col5 = Utils.getObjectValueInteger(planIdValueList.get(4)); // 点击
                col6 = Utils.getObjectValueDouble(planIdValueList.get(5)); // 花费
                col7 = Utils.getObjectValueDouble(planIdValueList.get(6)); // ctr
                col8 = Utils.getObjectValueDouble(planIdValueList.get(7)); // cpm
                col9 = Utils.getObjectValueDouble(planIdValueList.get(8)); // cpc
            }

            List<Object> planIdEcpmMap = planId8Map.get(datePlanIdKey);

            //拼接sql3的数据,广告位id
            String dateAdslotIdKey = col0 + "_" + planNameObjects.get(3);

            List<Object> listAdsoltId = adsoltId3Map.get(dateAdslotIdKey);
            double col10 = 0.0;
            if(planIdEcpmMap!= null && planIdEcpmMap.get(1)!=null){
                col10 = Utils.getObjectValueDouble(planIdEcpmMap.get(1));
            }
//            if (listAdsoltId != null) {
//                col10 = Utils.getObjectValueDouble(listAdsoltId.get(2)); //实际单价
//            }

            double col11 = 0.0;
            double col13 = 0.0;
            double col14 = 0.0;
            double col15 = 0.0;

//            double col12 = Utils.getObjectValueDouble(value.get(9)); //返佣金额
//            H23*0.3*AA23+
//            K23*L23*AB23+
//            N23*O23*AC23+
//            Z23*Y23*AD23
            double col12 = Utils.getObjectValueInteger(value.get(7)) * 0.3 * Utils.getObjectValueDouble(value.get(26)) +
                Utils.getObjectValueInteger(value.get(10)) * Utils.getObjectValueDouble(value.get(11)) * Utils.getObjectValueDouble(value.get(27)) +
                Utils.getObjectValueInteger(value.get(13)) * Utils.getObjectValueDouble(value.get(14)) * Utils.getObjectValueDouble(value.get(28)) +
                Utils.getObjectValueInteger(value.get(25)) * Utils.getObjectValueDouble(value.get(24)) * Utils.getObjectValueDouble(value.get(29));

            if (planIdValueList != null) {
//             col11 = new BigDecimal(col10).multiply(new BigDecimal(col4)).divide(new BigDecimal(1000),3).doubleValue();//实际消耗 (实际单价 * 曝光 / 1000)
                col11 = col10 * col4 / 1000;
                col13 = col12 - col11; //盈亏 (返佣金额 - 实际消耗)
//                col14 = new BigDecimal(col12).divide(new BigDecimal(col4),3).multiply(new BigDecimal(1000)).doubleValue();// ECPM  (返佣金额 / 曝光 * 1000 )
                col14 = col12 / col4 * 1000;
//                col15 = new BigDecimal(col12).divide(new BigDecimal(col5),3).doubleValue();// ecpc  返佣金额 / 点击
                col15 = col12 / col5;// ecpc  返佣金额 / 点击
            }


            //uv个数 = 当日有效拉新用户数 + 30日内有效召回用户数 + 30日有效召回用户数 + 60日有效召回用户数 + 90日有效召回用户数
            int col16 = Utils.getObjectValueInteger(value.get(3))
                + Utils.getObjectValueInteger(value.get(7))
                + Utils.getObjectValueInteger(value.get(13))
                + Utils.getObjectValueInteger(value.get(15));
            //剩下的3-29

            List<Object> return_ = new ArrayList<>();

            //新增几个,模版也跟着改变
            String col1_1 = col1.startsWith("m") ? "mau" : "dau";//类别
            String col1_2 = col3.toLowerCase().contains("ios") ? "ios" : "and";
            double col9_1 = col10 / (col12 / col5) / 1000;//回正CTR  =M4/(O4/G4)/1000
            double col13_1 = col13 / col11;//盈亏占比  =P4/N4
            double col14_1 = col10;//CPM成本  =M4
            double col15_1 = col11 / col5;// CPC成本 =N4/G4
            double col16_1 = 1.0 * col16 / col5;//CVR =V4/G4
            double col16_2 = col12 / col16;//UV价格  =O4/V4
            double col16_3 = col11 / col16;//UV成本 =N4/V4

            //20210401新增列
            int col16_4 = 0;
            int col16_5 = 0;
            int col16_6 = 0;
            double col16_7 = 0;
            double col16_8 = 0;
            if (col1.startsWith("qso")) {
                List<Object> query4ResList = planId4Map.get(datePlanIdKey);
                if (query4ResList != null) {
                    col16_4 = Utils.getObjectValueInteger(query4ResList.get(1)); //新客
                    col16_5 = Utils.getObjectValueInteger(query4ResList.get(2)); //首拉
                    col16_6 = Utils.getObjectValueInteger(query4ResList.get(3)); //mau
                    col16_7 = 1.0 * col16_5 / col5;// 首拉/点击  =AA9/G9
                    col16_8 = 1.0 * col16 / col16_5;// uv/ 首拉  =V9/AA9  V11/AA11
                }
            }


            //开始组装
            return_.add(col0);
            return_.add(col1);
//            return_.add(col2);
            return_.add(col1_1);
            return_.add(col1_2);
            return_.add(col3);
            return_.add(col4);
            return_.add(col5);
            return_.add(col6);
            return_.add(col7);
            return_.add(col8);
            return_.add(col9);
            return_.add(col9_1);
            return_.add(col10);
            return_.add(col11);
            return_.add(col12);
            return_.add(col13);
            return_.add(col13_1);
            return_.add(col14);
            return_.add(col14_1);
            return_.add(col15);
            return_.add(col15_1);
            return_.add(col16);
            return_.add(col16_1);
            return_.add(col16_2);
            return_.add(col16_3);
            //20210401新增
            return_.add(col16_4);
            return_.add(col16_5);
            return_.add(col16_6);
            return_.add(col16_7);
            return_.add(col16_8);

            return_.addAll(value.subList(3, value.size()));

            return return_;
        }).filter(Objects::nonNull)
            .collect(Collectors.toList());

        Workbook workbook = zd.generateZfbOutWorkBook(getTemplateInputStream("支付宝报告模版.xls"), "支付宝报告模版.xls", generatedList);

        return workbook;
    }

    @Override
    public Workbook parseZhifubaoExcelService2(InputStream is, String fileName) throws IOException {
        return null;
    }

    private InputStream getTemplateInputStream(String fileName) {
        InputStream in = this.getClass().getClassLoader().getResourceAsStream(fileName);
        return in;
    }


    //获取planid
    private Map<String, Map<String, List<Object>>> getAllPlanNameIDMap(List<List<Object>> list) {
        Map<String, List<Object>> allPlanNameIDMap = new HashMap<>();
        Map<String, List<Object>> allAdslocateIDMap = new HashMap<>();
        Map<String, List<Object>> allPlainIDMap = new HashMap<>();
        Map<String, Map<String, List<Object>>> _return = new HashMap<String, Map<String, List<Object>>>() {{
            put("allPlanNameID", allPlanNameIDMap);
            put("allAdslocateIDMap", allAdslocateIDMap);
            put("allPlainIDMap", allPlainIDMap);
        }};

        for (List<Object> list1 : list) {
            Object fullName = list1.get(1);

            if (fullName != null && !fullName.toString().equals("")) {
                String[] arr = fullName.toString().split("-");
                if (arr.length != 1) {
                    allPlanNameIDMap.put(arr[0], list1); //计划Name的Map
                    if (list1.get(3) != null) {
                        allAdslocateIDMap.put(list1.get(3).toString(), list1); //广告位id的Map
                    }
                    if(list1.get(0)!=null){
                        allPlainIDMap.put(list1.get(0).toString(),list1);
                    }
                }

            }
        }
        return _return;
    }



    //当出现重复的时候,标记一下
    private Pair<Map<String,List<Object>> , Set<String>> getAllPlanNameIDMap2(List<List<Object>> list) {
        Map<String, List<Object>> allPlanNameIDMap = new HashMap<>();
        Map<String,Set<String>> planNamePlanidsMap = new HashMap<>(); // 一个名称前缀下有几个planid,找出 数量大于1 的planid

//        Map<String, Integer> multiPlanIdMap = new HashMap<>();// planId,同名的计划名称
        Set<String> multiPlanNameSet = new HashSet<>();

        for (List<Object> list1 : list) {
            Object fullName = list1.get(1);

            if (fullName != null && !fullName.toString().equals("")) {
                String[] arr = fullName.toString().split("-");
                if (arr.length != 1) {
                    String planName = arr[0];
                    allPlanNameIDMap.put(planName, list1); //计划Name的Map
                    if(planNamePlanidsMap.containsKey(planName)){
                        planNamePlanidsMap.get(planName).add(list1.get(0).toString());
                    }else{
                        Set<String> planIds = new HashSet<String>(){{
                            add(list1.get(0).toString());
                        }};
                        planNamePlanidsMap.put(planName,planIds);
                    }
                }
            }
        }

        for(Map.Entry<String,Set<String>> entry:planNamePlanidsMap.entrySet()){
            String planName = entry.getKey();
            Set<String> planIds = entry.getValue();
            if(planIds.size()>1){
                multiPlanNameSet.add(planName);
            }
        }

//        planNamePlanidsMap.forEach((planName, planIds) -> {
//            int planIdsSize = planIds.size();
//            for (String plainId : planIds) {
//                multiPlanIdMap.put(plainId, planIdsSize); // planId有多少相同的planName
//            }
//        });


        return Pair.with(allPlanNameIDMap, multiPlanNameSet); //返回一个元组(plainid,adslotId);
    }

    //sql2的结果转换为map,sql3的结果转map  (token,list<Object>)
    private Map<String, List<Object>> getTokenMap(List<List<Object>> queryRes2) {
        Map<String, List<Object>> listMap = new HashMap<>();
        queryRes2.forEach(e -> {
            listMap.put(e.get(0).toString(), e);
        });
        return listMap;
    }


    @Override
    public Workbook parseTaobaoExcelService(InputStream is, String fileName) throws IOException, ParseException {
        int aduserId = 20970; //广告主id,大航海

        Map<String, List<String>> excelListMap = zd.parseTbInputExcel(is, fileName); //key: date + "_" + adSoltId;  日期全部是yyyy-MM-dd
        List<String> etlDateList = excelListMap.remove("etlDateSet"); //拿到处理时间
        List<List<Object>> res1 = zd.querySql5(etlDateList, aduserId); //投放计划,包名优化
        List<List<Object>> res2 = zd.querySql6(etlDateList, aduserId); //数据报告,投放统计,下载的数据
        List<List<Object>> res3 = zd.querySql7(etlDateList, aduserId); //数据报告,实时投放统计页面数据
        Map<String, List<Object>> tokenMap1 = getTokenMap(res1);// key为 date_计划id
        Map<String, List<Object>> tokenMap2 = getTokenMap(res2);// key为 date_计划id
        Map<String, List<Object>> tokenMap3 = getTokenMap(res3); // key为 date_计划id

        //转换tokenMap3
        List<List<Object>> adSoltPlanIdList = zd.QuerySQL1(); //所有的计划id,name,media_id,adsolcation_id

//        Map<String, Map<String, List<Object>>> map = getAllPlanNameIDMap(adSoltPlanIdList); //映射关系
//        Map<String, List<Object>> allPlanNameIDMap = map.get("allPlanNameID"); // 计划name, list //根据淘宝名称的前缀id(所谓淘宝的广告位id,非vlion库的id)
//
//        Map<String, List<Object>> allPlainIDMap = map.get("allPlainIDMap");  //计划id, list(list中包含了广告位id)

        Pair<Map<String, List<Object>>, Set<String>> allPlanNameIDMap2 = getAllPlanNameIDMap2(adSoltPlanIdList);
        Map<String, List<Object>> allPlanNameIDMap = allPlanNameIDMap2.getValue0();
        Set<String> multiPlanNames = allPlanNameIDMap2.getValue1(); //对应多个planId的planName


        //得到广告位id的map
//        Map<String, List<Object>> adsoltMap1 = getAdsoltMapFromPlanMap(tokenMap1,allPlainIDMap);
//        Map<String, List<Object>> adsoltMap2 = getAdsoltMapFromPlanMap(tokenMap2,allPlainIDMap);
//        Map<String, List<Object>> adsoltMap3 = getAdsoltMapFromPlanMap(tokenMap3,allPlainIDMap);


        //对上传的excel进行遍历,处理得到最终结果表
        List<List<Object>> outResList = excelListMap.entrySet().stream().map(m -> {
            Object[] outList =new Object[32];

            String key = m.getKey(); //日期_淘宝的id


            String[] keys = key.split("_");
            String date = keys[0];
            String adsoltIdTaobao = keys[1]; //是淘宝的广告位

            List<Object> objects = allPlanNameIDMap.get(adsoltIdTaobao); //根据名称拿到映射关系
            String newKey = null;
            String planId =null;
            if(objects!=null && objects.get(0) != null){
                planId = objects.get(0).toString(); //计划id
                newKey = date+"_"+planId; //
            }


            List<String> excelValueList = m.getValue();

            outList[0]=date;
            outList[1]=adsoltIdTaobao; // 计划id

            Object col4 =null;
            Object col5 =null;
            Object col6 =null;
            Object col7 =null;
            Object col9 =null;
            Object col17 =null;
            Object col10 =null;
            Object col11 =null;
            Object col12 = null;



            List<Object> list1 = tokenMap1.get(newKey);
            if(list1!=null){
                col12 = Utils.getObjectValueDouble(list1.get(1));// M列ECPM
                outList[12]=col12;// 成交价
            }

            List<Object> list2 = tokenMap2.get(newKey);
            if(list2 != null) {
                if (list2.get(2) != null) { // 获取操作系统
                    if(list2.get(2).toString().contains("-and-")){
                        outList[3]="and";
                    }else{
                        outList[3]="ios";
                    }
                }

                if(objects != null && objects.size() >= 4 && objects.get(3)!=null){
                    String realAdsoltId = objects.get(3).toString(); //真正的广告位id
                    col4 = realAdsoltId + " / " + excelValueList.get(8) + "-" + list2.get(2); //计划名称 广告位id-素材id-媒体-and/ios
                }else{
                    col4 =   "null / " + excelValueList.get(8) + "-" + list2.get(2); //计划名称 广告位id-素材id-媒体-and/ios
                }

                col5 = list2.get(8); // 曝光
                col6 = list2.get(9); // 点击
                col7 = Utils.getObjectValueDouble(list2.get(15))/ 100 ; // 点击率
                col9 = list2.get(11); // 出价 / ecpm
                outList[4]=col4;
                outList[5]=col5;
                outList[6]=col6;
                outList[7]=col7;
                outList[9]=col9;
            }


            List<Object> list3 = tokenMap3.get(newKey);
            if(list3 != null){
                col17 = list3.get(4); // 支付宝拉首唤
                col10 = list3.get(2); // 竞价率
                col11 = list3.get(3); //竞得率
                outList[17]=col17;
                outList[10]=col10;
                outList[11]=col11;
            }
            String col2 = excelValueList.get(8); //素材
            double col14 = Utils.getObjectValueDouble(excelValueList.get(7))/ 100;
            String col18 = excelValueList.get(5);
            String col19 = excelValueList.get(6);

            outList[2]=col2;//素材
            outList[14]=col14; // 预估佣金
            outList[18]=col18; // 总计促活量
            outList[19]=col19; // 目标粗活量

            // 间接计算出来的
            Object col8 = 0.0;
            Object col13 = 0.0;
            Object col15 = 0.0;
            Object col16 = 0.0;
            Object col20 = 0.0;
            Object col21 = 0.0;
            Object col22 = 0.0;
            Object col23 = 0.0;
            Object col24 = 0.0;
            Object col25 = 0.0;
            Object col26 = 0.0;
            Object col27 = 0.0;
            Object col28 = 0.0;
            Object col29 = 0.0;
            Object col30 = 0.0;
            Object col31 = 0.0;

            //下面需要计算
            //回正ctr,第8列  =M328/(O328/G328)/1000
            if(checkNumNotNull(col12, col14,col6)
                &&  checkNumNotZero(col14, col6)
            ){
                col8 = Utils.getObjectValueDouble(col12) / (Utils.getObjectValueDouble(col14)/Utils.getObjectValueDouble(16))/1000;
                outList[8]= col8;
            }

            //实际花费 col13=M292*F292/1000
            if(checkNumNotNull(col12, col5)
                &&  checkNumNotZero(col5)
            ){
                col13 = Utils.getObjectValueDouble(col12)*Utils.getObjectValueDouble(col5)/1000;
                outList[13]=col13;
            }

            //col15 盈亏= =O307-N307
            if(checkNumNotNull(col14, col13)
                &&  checkNumNotZero(col13)
            ){
                col15 = Utils.getObjectValueDouble(col14) - Utils.getObjectValueDouble(col13);
                outList[15]=col15;
            }

            //col16 roi  =O320/N320
            if(checkNumNotNull(col14,col13) && checkNumNotZero(col13)){
                col16 = Utils.getObjectValueDouble(col14) / Utils.getObjectValueDouble(col13);
                outList[16]=col16;
            }

            //col20  ecpm  =O293*1000/F293
            if(checkNumNotNull(col14,col5) && checkNumNotZero(col5)){
                col20 = Utils.getObjectValueDouble(col14)* 1000 / Utils.getObjectValueDouble(col5);
                outList[20]=col20;
            }

            //col21 =M306 实际CPM
            col21 = col12;
            outList[21]=col21;

            //col22 =O329/G329
            if(checkNumNotNull(col14,col6) && checkNumNotZero(col6)){
                col22 = Utils.getObjectValueDouble(col14)/Utils.getObjectValueDouble(col16);
                outList[22]=col22;
            }

            //col23 = =N330/G330
            if(checkNumNotNull(col13,col6) && checkNumNotZero(col6)){
                col23 = Utils.getObjectValueDouble(col13) / Utils.getObjectValueDouble(col6);
                outList[23]=col23;
            }

            //col24 = =R345/G345
            if(checkNumNotNull(col17,col6)&& checkNumNotZero(col6)){
                col24 = Utils.getObjectValueDouble(col17) / Utils.getObjectValueDouble(col6);
                outList[24]=col23;
            }

            //col25 = =S340/R340
            if(checkNumNotNull(col18, col17) && checkNumNotZero(col17)){
                col25 =  Utils.getObjectValueDouble(col18) / Utils.getObjectValueDouble(col17);
                outList[25]=col25;
            }

            //col26 =T292/R292
            if(checkNumNotNull(col19,col17) && checkNumNotZero(col17)){
                col26 =  Utils.getObjectValueDouble(col19) / Utils.getObjectValueDouble(col17);
                outList[26]=col26;
            }

            //col27 =S331/G331
            if(checkNumNotNull(col18,col6) && checkNumNotZero(col6)){
                col27 =  Utils.getObjectValueDouble(col18) / Utils.getObjectValueDouble(col6);
                outList[27]=col27;
            }

            //col28 =T340/G340
            if(checkNumNotNull(col19,col6) && checkNumNotZero(col6)){
                col28 =  Utils.getObjectValueDouble(col19) / Utils.getObjectValueDouble(col6);
                outList[28]=col28;
            }

            //col29 =T331/S331
            if(checkNumNotNull(col19,col18) && checkNumNotZero(col18)){
                col29 =  Utils.getObjectValueDouble(col19) / Utils.getObjectValueDouble(col18);
                outList[29]=col29;
            }

            //col30 =O340/T340
            if(checkNumNotNull(col14,col19) && checkNumNotZero(col19)){
                col30 =  Utils.getObjectValueDouble(col14) / Utils.getObjectValueDouble(col19);
                outList[30]=col30;
            }
            //col31 =N336/T336
            if(checkNumNotNull(col13,col19) && checkNumNotZero(col19)){
                col31 =  Utils.getObjectValueDouble(col13) / Utils.getObjectValueDouble(col19);
                outList[31]=col31;
            }

            return Arrays.asList(outList);
        }).collect(Collectors.toList());

        Workbook workbook = zd.generateTaobaoOutWorkBook(getTemplateInputStream("淘宝报告模版.xlsx"), "淘宝报告模版.xlsx", outResList,multiPlanNames);

        return workbook;
    }

    /**
     * 将plan的map改为广告位的map
     * @param planMap
     * @param allPlainIDMap
     */
    private Map<String,List<Object>> getAdsoltMapFromPlanMap(Map<String,List<Object>> planMap,Map<String,List<Object>> allPlainIDMap ){
        Map<String,List<Object>> res = new HashMap<>();
        for(Map.Entry<String,List<Object>> entry:planMap.entrySet()){
            String key = entry.getKey();
            List<Object> value = entry.getValue();

            String[] arr = key.split("_");
            String date = arr[0];
            String planId = arr[1];

            Object adsoltId = allPlainIDMap.get(planId).get(3);
            if(adsoltId!=null){
                res.put(date+"_"+adsoltId,value);
            }

        }
        return res;
    }

    //不为null 返回true
    private boolean checkNumNotNull(Object... objs){
        for(Object obj:objs){
            if(obj == null){
                return false;
            }
        }
        return true;
    }

    //不为0为true
    private boolean checkNumNotZero(Object... objs){
        for(Object obj:objs){
            if(Utils.getObjectValueDouble(obj) == 0.0){
                return false;
            }
        }
        return true;
    }




}
