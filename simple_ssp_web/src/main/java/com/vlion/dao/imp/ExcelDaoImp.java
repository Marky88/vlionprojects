package com.vlion.dao.imp;

import com.vlion.dao.ExcelDao;
import com.vlion.utils.DateUtil;
import com.vlion.utils.JdbcUtils;
import com.vlion.utils.Utils;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @description:
 * @author: malichun
 * @time: 2021/3/22/0022 15:54
 */
public class ExcelDaoImp implements ExcelDao {

    @Override
    public List<List<Object>> QuerySQL1() {
        String sql = "SELECT\n" +
            "    plan.id,\n" +
            "    plan.`name`, \n" +
            "    media.media_id,\n" +
            "    media.adslocation_id \n" +
            "FROM plan LEFT JOIN (\n" +
            "    SELECT\n" +
            "        map.plan_id,\n" +
            "        MAX( map.adslocation_id ) adslocation_id,\n" +
            "        MAX( ads.media_id ) media_id \n" +
            "    FROM\n" +
            "        plan_adslocation_map map,\n" +
            "        adslocation ads \n" +
            "    WHERE map.adslocation_id = ads.id \n" +
            "    GROUP BY map.plan_id \n" +
            ") AS media ON plan.id = media.plan_id";
        List<List<Object>> _return = new ArrayList<>();
        System.out.println("sqlSql1:");
        System.out.println(sql);

        JdbcUtils.queryBatch(sql, null, rs ->{
            while(rs.next()){
                List<Object> rsList = new ArrayList<>();
                for(int i=1;i<=4;i++){
                    rsList.add(rs.getObject(i));
                }
                _return.add(rsList);
            }
        });

        return _return;
    }

    @Override
    public List<List<Object>> QuerySQL2(List<String> etlDates,List<String> planIds) {
        String sql="SELECT\n" +
            "    CONCAT( time, '_', plan_id ) token, -- 匹配规则\n" +
            "    time, -- 统计时间 \n" +
            "    plan_id, -- 计划ID\n" +
            "    SUM( `show` ) `show`, -- 曝光 \n" +
            "    SUM( `click` ) `click`, -- 点击\n" +
            "    ROUND( SUM( cost / 1000 / 100 ), 2 ) cost, -- 花费\n" +
            "    ROUND( SUM( click ) / SUM( `show` ) * 100, 2 ) ctr, -- 点击率\n" +
            "    ROUND( SUM( cost / 1000 / 100 ) / SUM( `show` ) * 1000, 2 ) ecpm, -- cpm\n" +
            "    ROUND( SUM( cost / 1000 / 100 ) / SUM( click ), 2 ) cpc  -- cpc\n" +
            "FROM\n" +
//                "    `stat_day_dsp_realtime` \n" +
            "    `stat_day_dsp_offline` \n" +
            "WHERE\n" +
            "    time in ("+
            etlDates.stream().map(e -> {return "'"+e+"'";}).collect(Collectors.joining(","))
            +")  -- excel上传日志\n" +
            "    AND plan_id IN ( "+ String.join(",",planIds) +" )  -- excel上传的计划\n" +
            "GROUP BY time, plan_id ";

        List<List<Object>> _return = new ArrayList<>();

        JdbcUtils.queryBatch(sql, null, rs ->{
            while(rs.next()){
                int colCnt = rs.getMetaData().getColumnCount();
                List<Object> rsList = new ArrayList<>();
                for(int i=1;i<=colCnt;i++){
                    rsList.add(rs.getObject(i));
                }
                _return.add(rsList);
            }
        });

        return _return;
    }

    @Override
    public List<List<Object>> QuerySQL3(List<String> etlDates,List<String> adslocationIds) {
        String sql="SELECT\n" +
            "    CONCAT( time, '_', adslocation_id ) token, -- 匹配维度\n" +
            "    time, -- 统计时间\n" +
            "    ROUND( ( SUM( media_income ) ) / SUM( `show` ) * 1000, 2 ) 'ecpm'  --  实际单价\n" +
            "FROM\n" +
            "    `stat_day` \n" +
            "WHERE\n" +
            "    del = 'no' \n" +
            "    AND time in ("+
            etlDates.stream().map(e -> {return "'"+e+"'";}).collect(Collectors.joining(","))
            +" ) \n" +
            "    AND adslocation_id IN ( "+String.join(",",adslocationIds)+" ) \n" +
            "    AND channel_id = 79 \n" +
            "    AND is_upload = 'y' \n" +
            "GROUP BY time, adslocation_id";

        System.out.println("query3"+sql);

        List<List<Object>> _return = new ArrayList<>();

        JdbcUtils.queryBatch(sql, null, rs ->{
            while(rs.next()){
                int colCnt = rs.getMetaData().getColumnCount();
                List<Object> rsList = new ArrayList<>();
                for(int i=1;i<=colCnt;i++){
                    rsList.add(rs.getObject(i));
                }
                _return.add(rsList);
            }
        });

        return _return;
    }

    @Override
    public List<List<Object>> querySQL4(List<String> etlDates,List<String> planIds) {
        // 查询返回结果  token          |aduser_id|alp1|alp2 |alp4|
        String sql="select\n" +
            "concat(time,'_',plan_id) as token,\n" +
            " SUM( alp_1 ) alp1, -- 支付宝新登\n" +
            " SUM( alp_2 ) alp2, -- 支付宝拉活首唤\n" +
            " SUM( alp_4 ) alp4 -- 支付宝MAU\n" +
            "FROM\n" +
            " `stat_day_dsp_realtime` -- 数据表名称\n" +
            "WHERE\n" +
            " time in ("+etlDates.stream().map(e -> "'"+e+"'").collect(Collectors.joining(","))+")  -- 统计日期\n" +
            " and plan_id in ("+
            String.join(",",planIds)
            +")\n" +
            "GROUP BY -- 查询分类 [日期, 广告主, 计划]\n" +
            "concat(time,'_',plan_id) \n";

        List<List<Object>> _return = new ArrayList<>();

        JdbcUtils.queryBatch(sql, null, rs ->{
            while(rs.next()){
                int colCnt = rs.getMetaData().getColumnCount();
                List<Object> rsList = new ArrayList<>();
                for(int i=1;i<=colCnt;i++){
                    rsList.add(rs.getObject(i));
                }
                _return.add(rsList);
            }
        });

        return _return;
    }

    @Override
    public Map<String, List<Object>> parseZfbInputExcel(InputStream is, String fileName) throws IOException {
        Workbook workbook = null;
        Sheet sheet = null;
        Row row = null;
        Set<String> etlDateSet = new HashSet<>();

        //读取Excel输入流
        if (fileName.endsWith("xlsx")) {
            workbook = new XSSFWorkbook(is);
        } else {
            workbook = new HSSFWorkbook(is);
        }

        Map<String, List<Object>> _return = new LinkedHashMap<>();

        //获取Excel表单
        sheet = workbook.getSheetAt(0);
        //从第一行开始
        for( int rowNum = 1;rowNum <= sheet.getLastRowNum();rowNum++ ){
            row = sheet.getRow(rowNum);
            List<Object> rowList = new ArrayList<>(row.getLastCellNum()+1);
            String date = row.getCell(0).getStringCellValue();
            etlDateSet.add(date);
            String channel = row.getCell(1).getStringCellValue();
            rowList.add(date); //日期
            rowList.add(channel); //二级渠道
            for(int i=2;i< row.getLastCellNum();i++){
                rowList.add(row.getCell(i).getStringCellValue());
            }
            //选取当前返佣金额大的1条  Q列 0 - 16
            String key = date+"_"+channel;
            if(_return.containsKey(key)){
                double col16New = Double.parseDouble(rowList.get(16).toString());//取当日返现金额大的
                double col16Old = Double.parseDouble(_return.get(key).get(16).toString());
                if(col16New > col16Old){
                    _return.put(key,rowList);
                }
            }else{
                _return.put(key,rowList);
            }

        }
        //把日期放进去
        _return.put("etlDateSet", new ArrayList<>(etlDateSet));
        return _return;
    }

    @Override
    public Workbook generateZfbOutWorkBook(InputStream is, String fileName, List<List<Object>> datas) throws IOException {
        Workbook workbook = null;
        Sheet sheet = null;
        Row row = null;

        //读取Excel输入流
        if (fileName.endsWith("xlsx")) {
            workbook = new XSSFWorkbook(is);
        } else {
            workbook = new HSSFWorkbook(is);
        }
        //带边框style
        CellStyle borderStyle = workbook.createCellStyle();
        borderStyle.setBorderBottom(BorderStyle.THIN);
        borderStyle.setBorderLeft(BorderStyle.THIN);
        borderStyle.setBorderRight(BorderStyle.THIN);
        borderStyle.setBorderTop(BorderStyle.THIN);

        //百分号Style
        CellStyle perCentStype = workbook.createCellStyle();
        perCentStype.setBorderBottom(BorderStyle.THIN);
        perCentStype.setBorderLeft(BorderStyle.THIN);
        perCentStype.setBorderRight(BorderStyle.THIN);
        perCentStype.setBorderTop(BorderStyle.THIN);
        perCentStype.setDataFormat((short) BuiltinFormats.getBuiltinFormat("0.00%"));

        //获取Excel表单
        sheet = workbook.getSheetAt(0);

        //一些sum值
        int sum4 = 0;
        int sum5 =0;
        double sum6 =0;
        double sum7 = 0;
        double sum8 = 0;
        double sum9 = 0;
        double sum11 = 0;
        double sum12 = 0;
        double sum13 = 0;
        double sum14 = 0;
        double sum15 = 0;
        int sum16 =0;


        for(int j=0;j<datas.size();j++) {
//            System.out.println("处理第"+j+"行");
            //每一行数据
            List<Object> data = datas.get(j);

            row = sheet.createRow(j+2);
//            System.out.println(Utils.getObjectValueString(data.get(0)));
            row.createCell(0).setCellValue(Utils.getObjectValueString(data.get(0)));
            row.createCell(1).setCellValue(Utils.getObjectValueString(data.get(1)));
            row.createCell(2).setCellValue(Utils.getObjectValueString(data.get(2)));
            row.createCell(3).setCellValue(Utils.getObjectValueString(data.get(3)));
            row.createCell(4).setCellValue(Utils.getObjectValueString(data.get(4)));
            row.createCell(5).setCellValue(Utils.getObjectValueInteger(data.get(5)));
            row.createCell(6).setCellValue(Utils.getObjectValueInteger(data.get(6)));
            row.createCell(7).setCellValue(Utils.getObjectValueDouble(data.get(7)));
            row.createCell(8).setCellValue(Utils.getObjectValueDouble(data.get(8)));
            row.createCell(9).setCellValue(Utils.getObjectValueDouble(data.get(9)));
            row.createCell(10).setCellValue(Utils.getObjectValueDouble(data.get(10)));
            row.createCell(11).setCellValue(Utils.getObjectValueDouble(data.get(11)));
            row.createCell(12).setCellValue(Utils.getObjectValueDouble(data.get(12)));
            row.createCell(13).setCellValue(Utils.getObjectValueDouble(data.get(13)));
            row.createCell(14).setCellValue(Utils.getObjectValueDouble(data.get(14)));
            row.createCell(15).setCellValue(Utils.getObjectValueDouble(data.get(15)));
            row.createCell(16).setCellValue(Utils.getObjectValueDouble(data.get(16)));
            row.createCell(17).setCellValue(Utils.getObjectValueDouble(data.get(17)));
            row.createCell(18).setCellValue(Utils.getObjectValueDouble(data.get(18)));
            row.createCell(19).setCellValue(Utils.getObjectValueDouble(data.get(19)));
            row.createCell(20).setCellValue(Utils.getObjectValueDouble(data.get(20)));
            row.createCell(21).setCellValue(Utils.getObjectValueInteger(data.get(21)));
            row.createCell(22).setCellValue(Utils.getObjectValueDouble(data.get(22)));
            row.createCell(23).setCellValue(Utils.getObjectValueDouble(data.get(23)));
            row.createCell(24).setCellValue(Utils.getObjectValueDouble(data.get(24)));
            //20210401新增
            row.createCell(25).setCellValue(Utils.getObjectValueInteger(data.get(25)));
            row.createCell(26).setCellValue(Utils.getObjectValueInteger(data.get(26)));
            row.createCell(27).setCellValue(Utils.getObjectValueInteger(data.get(27)));
            row.createCell(28).setCellValue(Utils.getObjectValueDouble(data.get(28)));
            row.createCell(29).setCellValue(Utils.getObjectValueDouble(data.get(29)));


            row.createCell(30).setCellValue(Utils.getObjectValueInteger(data.get(30)));
            row.createCell(31).setCellValue(Utils.getObjectValueDouble( data.get(31)));
            row.createCell(32).setCellValue(Utils.getObjectValueDouble( data.get(32)));
            row.createCell(33).setCellValue(Utils.getObjectValueDouble( data.get(33)));
            row.createCell(34).setCellValue(Utils.getObjectValueInteger(data.get(34)));
            row.createCell(35).setCellValue(Utils.getObjectValueDouble( data.get(35)));
            row.createCell(36).setCellValue(Utils.getObjectValueDouble( data.get(36)));
            row.createCell(37).setCellValue(Utils.getObjectValueInteger(data.get(37)));
            row.createCell(38).setCellValue(Utils.getObjectValueDouble( data.get(38)));
            row.createCell(39).setCellValue(Utils.getObjectValueDouble( data.get(39)));
            row.createCell(40).setCellValue(Utils.getObjectValueInteger(data.get(40)));
            row.createCell(41).setCellValue(Utils.getObjectValueDouble( data.get(41)));
            row.createCell(42).setCellValue(Utils.getObjectValueDouble( data.get(42)));
            row.createCell(43).setCellValue(Utils.getObjectValueDouble( data.get(43)));
            row.createCell(44).setCellValue(Utils.getObjectValueDouble( data.get(44)));
            row.createCell(45).setCellValue(Utils.getObjectValueDouble( data.get(45)));
            row.createCell(46).setCellValue(Utils.getObjectValueInteger(data.get(46)));
            row.createCell(47).setCellValue(Utils.getObjectValueInteger(data.get(47)));
            row.createCell(48).setCellValue(Utils.getObjectValueDouble( data.get(48)));
            row.createCell(49).setCellValue(Utils.getObjectValueDouble( data.get(49)));
            row.createCell(50).setCellValue(Utils.getObjectValueDouble( data.get(50)));
            row.createCell(51).setCellValue(Utils.getObjectValueDouble( data.get(51)));
            row.createCell(52).setCellValue(Utils.getObjectValueInteger(data.get(52)));
            row.createCell(53).setCellValue(Utils.getObjectValueDouble(data.get(53)));
            row.createCell(54).setCellValue(Utils.getObjectValueDouble(data.get(54)));
            row.createCell(55).setCellValue(Utils.getObjectValueDouble(data.get(55)));
            row.createCell(56).setCellValue(Utils.getObjectValueDouble(data.get(56)));

            //设置样式
            for(int i=0;i<56;i++){
                if(i == 11 || i == 22){
                    row.getCell(i).setCellStyle(perCentStype);  //设置百分号
                    continue;
                }
//                System.out.println(row.getCell(i));
                row.getCell(i).setCellStyle(borderStyle);
            }

            //处理sum值
            sum4 += Utils.getObjectValueInteger(data.get(5));
            sum5 += Utils.getObjectValueInteger(data.get(6));
            sum6 += Utils.getObjectValueDouble(data.get(7));
            sum7 += Utils.getObjectValueDouble(data.get(8));
            sum8 += Utils.getObjectValueDouble(data.get(9));
            sum9 += Utils.getObjectValueDouble(data.get(10));
            sum11 += Utils.getObjectValueDouble(data.get(13));
            sum12 += Utils.getObjectValueDouble(data.get(14));
            sum13 += Utils.getObjectValueDouble(data.get(15));
            sum14 += Utils.getObjectValueDouble(data.get(17));
            sum15 += Utils.getObjectValueDouble(data.get(19));
            sum16 += Utils.getObjectValueInteger(data.get(21));
        }
        CellStyle sumStyle = workbook.createCellStyle();
        sumStyle.setBorderBottom(BorderStyle.THIN);
        sumStyle.setBorderLeft(BorderStyle.THIN);
        sumStyle.setBorderRight(BorderStyle.THIN);
        sumStyle.setBorderTop(BorderStyle.THIN);
        sumStyle.setVerticalAlignment(VerticalAlignment.CENTER);
        sumStyle.setAlignment(HorizontalAlignment.CENTER);

        CellStyle sumStyle2 = workbook.createCellStyle();  // 带百分号
        sumStyle2.setBorderBottom(BorderStyle.THIN);
        sumStyle2.setBorderLeft(BorderStyle.THIN);
        sumStyle2.setBorderRight(BorderStyle.THIN);
        sumStyle2.setBorderTop(BorderStyle.THIN);
        sumStyle2.setVerticalAlignment(VerticalAlignment.CENTER);
        sumStyle2.setAlignment(HorizontalAlignment.CENTER);
        sumStyle2.setDataFormat((short) BuiltinFormats.getBuiltinFormat("0.00%"));

        //计算求和
        row = sheet.getRow(0);
        row.createCell(5).setCellValue(sum4);
        row.createCell(6).setCellValue(sum5);
        row.createCell(7).setCellValue(sum6);
//        row.createCell(7).setCellValue(new BigDecimal(sum5).divide(new BigDecimal(sum4),3).doubleValue()); // 点击 / 曝光
        row.createCell(8).setCellValue((double) sum5 / sum4); // 点击 / 曝光
//        row.createCell(8).setCellValue(new BigDecimal(sum6).divide(new BigDecimal(sum4),3).multiply(new BigDecimal(1000)).doubleValue());  //花费 / 曝光 * 1000
        row.createCell(9).setCellValue( (sum6)/ sum4 *1000);  //花费 / 曝光 * 1000

//        row.createCell(9).setCellValue(new BigDecimal(sum6).divide(new BigDecimal(sum5),3).doubleValue()); // 花费 / 点击
        row.createCell(10).setCellValue(sum6 / sum5); // 花费 / 点击

        row.createCell(13).setCellValue(sum11);
        row.createCell(14).setCellValue(sum12);
        row.createCell(15).setCellValue(sum12 - sum11); // 返佣金额 - 实际消耗
//        row.createCell(14).setCellValue(new BigDecimal(sum12).divide(new BigDecimal(sum4),3).multiply(new BigDecimal(1000)).doubleValue());  // 返佣金额 / 曝光 * 1000
        row.createCell(17).setCellValue(sum12/sum4 * 1000);  // 返佣金额 / 曝光 * 1000
        row.createCell(19).setCellValue(sum12 / sum5);
        row.createCell(21).setCellValue(sum16);

        //新加汇总字段
        double aggCol10 =  sum11 / sum4 * 1000;  // M列
        double aggCol9_1 = aggCol10 / (sum12/sum5) / 1000;//L列  =M1/(O1/G1)/1000
        double aggCol13_1 = sum13 / sum11;//Q列=P1/N1
        double aggCol14_1 = aggCol10; //S列  =M1
        double aggCol15_1 = sum11 / sum5;//U列=N1/G1
        double aggCol16_1 = 1.0 * sum16 / sum5;//W列 =V1/G1
        double aggCol16_2 = sum12 / sum16;//X列 =O1/V1
        double aggCol16_3 = sum11 / sum16;//Y列  =N1/V1

        row.createCell(12).setCellValue(aggCol10);
        row.createCell(11).setCellValue(aggCol9_1);
        row.createCell(16).setCellValue(aggCol13_1);
        row.createCell(18).setCellValue(aggCol14_1);
        row.createCell(20).setCellValue(aggCol15_1);

        row.createCell(22).setCellValue(aggCol16_1);
        row.createCell(23).setCellValue(aggCol16_2);
        row.createCell(24).setCellValue(aggCol16_3);

        row.getCell(5).setCellStyle(sumStyle);
        row.getCell(6).setCellStyle(sumStyle);
        row.getCell(7).setCellStyle(sumStyle);
        row.getCell(8).setCellStyle(sumStyle);
        row.getCell(9).setCellStyle(sumStyle);
        row.getCell(10).setCellStyle(sumStyle);
        row.getCell(13).setCellStyle(sumStyle);
        row.getCell(14).setCellStyle(sumStyle);
        row.getCell(15).setCellStyle(sumStyle);
        row.getCell(17).setCellStyle(sumStyle);
        row.getCell(19).setCellStyle(sumStyle);
        row.getCell(21).setCellStyle(sumStyle);
        //新加汇总字段样式
        row.getCell(12).setCellStyle(sumStyle);
        row.getCell(11).setCellStyle(sumStyle2);
        row.getCell(16).setCellStyle(sumStyle);
        row.getCell(18).setCellStyle(sumStyle);
        row.getCell(20).setCellStyle(sumStyle);
        row.getCell(22).setCellStyle(sumStyle2);
        row.getCell(23).setCellStyle(sumStyle);
        row.getCell(24).setCellStyle(sumStyle);

        return workbook;
    }

    @Override
    public Map<String, List<Object>> parseZfbInputExcel2(InputStream is, String fileName) throws IOException {
        Workbook workbook = null;
        Sheet sheet = null;
        Row row = null;
        Set<String> etlDateSet = new HashSet<>();

        //读取Excel输入流
        if (fileName.endsWith("xlsx")) {
            workbook = new XSSFWorkbook(is);
        } else {
            workbook = new HSSFWorkbook(is);
        }

        Map<String, List<Object>> _return = new LinkedHashMap<>();

        //获取Excel表单
        sheet = workbook.getSheetAt(0);
        //从第一行开始
        for( int rowNum = 1;rowNum <= sheet.getLastRowNum();rowNum++ ){
            row = sheet.getRow(rowNum);
            List<Object> rowList = new ArrayList<>(row.getLastCellNum()+1);
            String date = row.getCell(0).getStringCellValue();
            etlDateSet.add(date);
            String channel = row.getCell(1).getStringCellValue();
            rowList.add(date); //日期
            rowList.add(channel); //二级渠道
            for(int i=2;i< row.getLastCellNum();i++){
                rowList.add(row.getCell(i).getStringCellValue());
            }
            //选取当前返佣金额大的1条  Q列 0 - 16
            String key = date+"_"+channel;
            if(_return.containsKey(key)){
                double col16New = Double.parseDouble(rowList.get(16).toString());//取当日返现金额大的
                double col16Old = Double.parseDouble(_return.get(key).get(16).toString());
                if(col16New > col16Old){
                    _return.put(key,rowList);
                }
            }else{
                _return.put(key,rowList);
            }

        }
        //把日期放进去
        _return.put("etlDateSet", new ArrayList<>(etlDateSet));
        return _return;
    }

    @Override
    public Workbook generateZfbOutWorkBook2(InputStream is, String fileName, List<List<Object>> datas) throws IOException {
        return null;
    }

    @Override
    public Map<String, List<String>> parseTbInputExcel(InputStream is, String fileName) throws IOException, ParseException {
        Map<String, List<String>> _return = new LinkedHashMap<>();
        //看一个Excel中有哪些日期
        Set<String> etlDateSet = new HashSet<>();

        if(fileName.endsWith(".csv")){  //如果是csv格式
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String firstLine = br.readLine();
            String[] cols = firstLine.split(",");
            int colsNum = cols.length;
            String line = null;
            while((line = br.readLine()) != null){
                List<String> rowList = new ArrayList<>(colsNum);
                String[] arr = line.split(",",-1);
                if(arr.length!= colsNum){
                    throw new IOException(line+" 错误");
                }
                String date = arr[0];
                String date2 = DateUtil.formatOldStrToNewStr(date,DateUtil.DATE_FORMAT2, DateUtil.DATE_FORMAT1);
                etlDateSet.add(date2);
                String adSoltId = arr[1];
                rowList.add(date2); //日期
                rowList.add(adSoltId); //广告位id
                for (int i = 2; i < colsNum; i++) {
                    rowList.add(arr[i]);
                }
                String key = date2 + "_" + adSoltId;
                if(_return.containsKey(key)){
                    //FGH相加
                    List<String> beforeRowList = _return.get(key);
                    if(beforeRowList.get(5)!=null && rowList.get(5)!=null){
                        beforeRowList.set(5,String.valueOf(Utils.getObjectValueInteger(beforeRowList.get(5))+Utils.getObjectValueInteger(rowList.get(5))));
                    }
                    if(beforeRowList.get(6)!=null && rowList.get(6)!=null){
                        beforeRowList.set(6,String.valueOf(Utils.getObjectValueInteger(beforeRowList.get(6))+Utils.getObjectValueInteger(rowList.get(6))));
                    }
                    if(beforeRowList.get(7)!=null && rowList.get(7)!=null){
                        beforeRowList.set(7,String.valueOf(Utils.getObjectValueInteger(beforeRowList.get(7))+Utils.getObjectValueInteger(rowList.get(7))));
                    }
                }else{
                    _return.put(key, rowList);
                }
            }
        }else { //是excel文件
            Workbook workbook = null;
            Sheet sheet = null;
            Row row = null;


            //读取Excel输入流
            if (fileName.endsWith("xlsx")) {
                workbook = new XSSFWorkbook(is);
            } else {
                workbook = new HSSFWorkbook(is);
            }



            //获取Excel表单
            sheet = workbook.getSheetAt(0);

            //从第一行开始
            for (int rowNum = 1; rowNum <= sheet.getLastRowNum(); rowNum++) {
                row = sheet.getRow(rowNum);
                List<String> rowList = new ArrayList<>(row.getLastCellNum() + 1);
                Cell cell = row.getCell(0);
                cell.setCellType(CellType.STRING);
                String date = cell.getStringCellValue();
                String date2 = DateUtil.formatOldStrToNewStr(date,DateUtil.DATE_FORMAT2, DateUtil.DATE_FORMAT1);
                etlDateSet.add(date2);
                Cell cell1 = row.getCell(1);
                cell1.setCellType(CellType.STRING);
                String adSoltId = cell1.getStringCellValue();
                rowList.add(date2); //日期
                rowList.add(adSoltId); //广告位id

                for (int i = 2; i < row.getLastCellNum(); i++) {
                    Cell celli = row.getCell(i);
                    celli.setCellType(CellType.STRING);
                    rowList.add(celli.getStringCellValue());
                }
                String key = date2 + "_" + adSoltId;
                if(_return.containsKey(key)){
                    //FGH相加
                    List<String> beforeRowList = _return.get(key);
                    if(beforeRowList.get(5)!=null && rowList.get(5)!=null){
                        beforeRowList.set(5,String.valueOf(Utils.getObjectValueInteger(beforeRowList.get(5))+Utils.getObjectValueInteger(rowList.get(5))));
                    }
                    if(beforeRowList.get(6)!=null && rowList.get(6)!=null){
                        beforeRowList.set(6,String.valueOf(Utils.getObjectValueInteger(beforeRowList.get(6))+Utils.getObjectValueInteger(rowList.get(6))));
                    }
                    if(beforeRowList.get(7)!=null && rowList.get(7)!=null){
                        beforeRowList.set(7,String.valueOf(Utils.getObjectValueInteger(beforeRowList.get(7))+Utils.getObjectValueInteger(rowList.get(7))));
                    }
                }else{
                    _return.put(key, rowList);
                }
            }
        }
        //把日期放进去
        _return.put("etlDateSet", new ArrayList<>(etlDateSet));

        return _return;
    }

    @Override
    public List<List<Object>> querySql5(List<String> etlDates, int aduserId) { // aduserId 20970
        String sql = "SELECT\n" +
            "concat(time,'_',plan_id) as token," +
            " ROUND( SUM( price ) / SUM( `show` ) * 1000, 2 ) ecpm,  -- 页面对应 汇总 ecpm\n" +
            " sum(`show`) as `show`\n" +
            "FROM\n" +
            " `stat_oppkg` -- 查询表\n" +
            "WHERE\n" +
            " time in ("+
            etlDates.stream().map(s -> "'"+s+"'").collect(Collectors.joining())
            +")  -- 过滤时间\n" +
            " and aduser_id = ?\n" +
            "group by concat(time,'_',plan_id) \n" +
            "having not(`show` is null or `show` = 0)";
        System.out.println("querySql5=======");
        System.out.println(sql);

        List<List<Object>> _return = new ArrayList<>();

        JdbcUtils.queryBatch(sql, new Object[]{ aduserId }, rs ->{
            while(rs.next()){
                int colCnt = rs.getMetaData().getColumnCount();
                List<Object> rsList = new ArrayList<>();
                for(int i=1;i<=colCnt;i++){
                    rsList.add(rs.getObject(i));
                }
                _return.add(rsList);
            }
        });
        return _return;
    }

    @Override
    public List<List<Object>> querySql6(List<String> etlDates, int aduserId) {
//        String sql = "SELECT\n" +
//                "    concat(main.time,'_',main.adslocation_id) as token,\n" +
//                "    main.plan_id,"+
//                "    aduser.nickname , -- 广告主名称\n" +
//                "    os.name as os_name,\n" +
//                "    plan.`name` , -- 计划名称\n" +
//                "    main.*\n" +
//                "FROM (\n" +
//                "    SELECT\n" +
//                "        time,-- 日期\n" +
//                "        aduser_id,-- 广告主ID\n" +
//                "        plan_id,-- 计划ID\n" +
//                "        adslocation_id , -- 广告位ID\n" +
//                "        SUM( req ) req,-- 请求\n" +
//                "        SUM( bid ) bid,-- 竞价\n" +
//                "        SUM( `show` ) `show`,-- 曝光\n" +
//                "        SUM( click ) click,-- 点击\n" +
//                "        ROUND( SUM( cost / 1000 / 100 ), 2 ) cost,-- 花费\n" +
//                "        ROUND( SUM( cost / 1000 / 100 ) / SUM( `show` ) * 1000, 2 ) ecpm,-- ecpm\n" +
//                "        ROUND( SUM( cost / 1000 / 100 ) / SUM( click ), 2 ) cpc,-- cpc\n" +
//                "        ROUND( SUM( bid ) / SUM( req ) * 100, 2 ) bidRate,-- 竞价率\n" +
//                "        ROUND( SUM( `show` ) / SUM( bid ) * 100, 2 ) bidSuccessRate,-- 竞价成功率\n" +
//                "        ROUND( SUM( click ) / SUM( `show` ) * 100, 2 ) ctr -- 点击率 \n" +
//                "    FROM stat_day_dsp_offline FORCE INDEX ( find ) -- 查询表\n" +
//                "    WHERE time in ("+
//                etlDates.stream().map(s -> "'"+s+"'").collect(Collectors.joining())
//                +") -- 日期过滤\n" +
//                "    AND aduser_id IN ( ? ) -- 广告主ID     \n" +
//                "    GROUP BY-- 分组 (日期, 广告主ID, 计划ID)\n" +
//                "        `time`,\n" +
//                "        `aduser_id`,\n" +
//                "        `plan_id` ,\n" +
//                "        adslocation_id \n" +
//                "    ORDER BY\n" +
//                "        `req` DESC -- 排序, <请求>\n" +
//                ") AS main\n" +
//                "LEFT JOIN `user` aduser ON aduser.id = main.aduser_id\n" +
//                "LEFT JOIN plan ON plan.id = main.plan_id\n" +
//                "left join adslocation ad on main.adslocation_id = ad.id\n" +
//                "left join os on os.id = ad.os_id ";
        String sql = "select\n" +
            "    concat(time,'_',plan_id) as token,\n" +
            "    aduser.nickname , -- 广告主名称\n" +
            "    plan.`name`, -- 计划名称\n" +
            "    main.* \n" +
            "FROM (\n" +
            "    SELECT\n" +
            "        time,-- 日期\n" +
            "        aduser_id,-- 代码位ID\n" +
            "        plan_id,-- 计划ID\n" +
            "        SUM( req ) req,-- 请求\n" +
            "        SUM( bid ) bid,-- 竞价\n" +
            "        SUM( `show` ) `show`,-- 曝光\n" +
            "        SUM( click ) click,-- 点击\n" +
            "        ROUND( SUM( cost / 1000 / 100 ), 2 ) cost,-- 花费\n" +
            "        ROUND( SUM( cost / 1000 / 100 ) / SUM( `show` ) * 1000, 2 ) ecpm,-- ecpm\n" +
            "        ROUND( SUM( cost / 1000 / 100 ) / SUM( click ), 2 ) cpc,-- cpc\n" +
            "        ROUND( SUM( bid ) / SUM( req ) * 100, 2 ) bidRate,-- 竞价率\n" +
            "        ROUND( SUM( `show` ) / SUM( bid ) * 100, 2 ) bidSuccessRate,-- 竞价成功率\n" +
            "        ROUND( SUM( click ) / SUM( `show` ) * 100, 2 ) ctr -- 点击率 \n" +
            "    FROM stat_day_dsp_offline FORCE INDEX ( find ) -- 查询表\n" +
            "    WHERE time in ("+
            etlDates.stream().map(s -> "'"+s+"'").collect(Collectors.joining())
            +") -- 日期过滤\n" +
            "    AND aduser_id IN ( ? ) -- 广告主ID     \n" +
            "    GROUP BY-- 分组 (日期, 广告主ID, 计划ID)\n" +
            "        `time`,\n" +
            "        `aduser_id`,\n" +
            "        `plan_id` \n" +
            "    ORDER BY\n" +
            "        `req` DESC -- 排序, <请求>\n" +
            ") AS main\n" +
            "LEFT JOIN `user` aduser ON aduser.id = main.aduser_id\n" +
            "LEFT JOIN plan ON plan.id = main.plan_id";
        System.out.println("querySql6=======");
        System.out.println(sql);

        List<List<Object>> _return = new ArrayList<>();
        JdbcUtils.queryBatch(sql, new Object[]{ aduserId }, rs ->{
            while(rs.next()){
                int colCnt = rs.getMetaData().getColumnCount();
                List<Object> rsList = new ArrayList<>();
                for(int i=1;i<=colCnt;i++){
                    rsList.add(rs.getObject(i));
                }
                _return.add(rsList);
            }
        });
        return _return;

    }

    @Override
    public List<List<Object>> querySql7(List<String> etlDates, int aduserId) {
        String sql ="SELECT\n" +
            "    concat(time,'_',plan_id) as token, -- 时间_计划id \n" +
            "    aduser_id, -- 广告主ID\n" +
            "    ROUND( SUM( bid ) / SUM( req ) * 100, 2 ) bidRate, -- 竞价绿\n" +
            "    ROUND( SUM( `show` ) / SUM( bid ) * 100, 2 ) bidSuccessRate, -- 竞价成功率\n" +
            "    SUM(alp_2) alp2cr  -- 支付宝拉活首唤\n" +
            "FROM\n" +
            "    `stat_day_dsp_realtime` \n" +
            "WHERE\n" +
            "    time in ("+
            etlDates.stream().map(s -> "'"+s+"'").collect(Collectors.joining())
            +")  -- 统计时间\n" +
            "    AND aduser_id IN ( ? )  -- 广告主ID \n" +
            "GROUP BY -- 分组 (时间, 广告主ID, 计划ID)\n" +
            "    `time`,\n" +
            "    `aduser_id`,\n" +
            "    `plan_id` \n" +
            "ORDER BY `bidRate` DESC -- 排序";
        System.out.println("querySql7=======");
        System.out.println(sql);

        List<List<Object>> _return = new ArrayList<>();
        JdbcUtils.queryBatch(sql, new Object[]{ aduserId }, rs ->{
            while(rs.next()){
                int colCnt = rs.getMetaData().getColumnCount();
                List<Object> rsList = new ArrayList<>(colCnt);
                for(int i=1;i<=colCnt;i++){
                    rsList.add(rs.getObject(i));
                }
                _return.add(rsList);
            }
        });
        return _return;

    }

    @Override
    public Workbook generateTaobaoOutWorkBook(InputStream is, String fileName, List<List<Object>> datas,Set<String> multiPlanNames) throws IOException {
        Workbook workbook = null;
        Sheet sheet = null;
        Row row = null;

        //读取Excel输入流
        if (fileName.endsWith("xlsx")) {
            workbook = new XSSFWorkbook(is);
        } else {
            workbook = new HSSFWorkbook(is);
        }
        //带边框style
        CellStyle borderStyle = workbook.createCellStyle();
        borderStyle.setBorderBottom(BorderStyle.THIN);
        borderStyle.setBorderLeft(BorderStyle.THIN);
        borderStyle.setBorderRight(BorderStyle.THIN);
        borderStyle.setBorderTop(BorderStyle.THIN);

        //百分号Style
        CellStyle perCentStype = workbook.createCellStyle();
        perCentStype.setBorderBottom(BorderStyle.THIN);
        perCentStype.setBorderLeft(BorderStyle.THIN);
        perCentStype.setBorderRight(BorderStyle.THIN);
        perCentStype.setBorderTop(BorderStyle.THIN);
        perCentStype.setDataFormat((short) BuiltinFormats.getBuiltinFormat("0.00%"));

        //标红的style
        CellStyle redStyle = workbook.createCellStyle();
        redStyle.setBorderBottom(BorderStyle.THIN);
        redStyle.setBorderLeft(BorderStyle.THIN);
        redStyle.setBorderRight(BorderStyle.THIN);
        redStyle.setBorderTop(BorderStyle.THIN);
        redStyle.setFillForegroundColor(IndexedColors.RED.getIndex());
        redStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);

        //获取Excel表单
        sheet = workbook.getSheetAt(0);

        //一些sum值
        int sum5 =0;
        int sum6 =0;
        double rate7=0.0;
        double rate12=0.0;
        double sum13 = 0.0;
        double sum14 = 0.0;
        double sum15 = 0.0;
        double sum18 = 0.0;
        double sum19 = 0.0;


        for(int j=0;j<datas.size();j++) {
//            System.out.println("处理第"+j+"行");
            //每一行数据
            List<Object> data = datas.get(j);

            row = sheet.createRow(j+2);

            String  col0    = Utils.getObjectValueString(data.get(0));
            String  col1    = Utils.getObjectValueString(data.get(1));
            String  col2    = Utils.getObjectValueString(data.get(2));
            String  col3    = Utils.getObjectValueString(data.get(3));
            String  col4    = Utils.getObjectValueString(data.get(4));
            int  col5    = Utils.getObjectValueInteger(data.get(5));
            int  col6    = Utils.getObjectValueInteger(data.get(6));
            double  col7    = Utils.getObjectValueDouble(data.get(7));
            double  col8    = Utils.getObjectValueDouble(data.get(8));
            double  col9    = Utils.getObjectValueDouble(data.get(9));
            double  col10    = Utils.getObjectValueDouble(data.get(10));
            double  col11    = Utils.getObjectValueDouble(data.get(11));
            double  col12    = Utils.getObjectValueDouble(data.get(12));
            double  col13    = Utils.getObjectValueDouble(data.get(13));
            double  col14    = Utils.getObjectValueDouble(data.get(14));
            double  col15    = Utils.getObjectValueDouble(data.get(15));
            double  col16    = Utils.getObjectValueDouble(data.get(16));
            int  col17    = Utils.getObjectValueInteger(data.get(17));
            int  col18    = Utils.getObjectValueInteger(data.get(18));
            int  col19    = Utils.getObjectValueInteger(data.get(19));
            double  col20    = Utils.getObjectValueDouble(data.get(20));
            double  col21    = Utils.getObjectValueInteger(data.get(21));
            double  col22    = Utils.getObjectValueDouble(data.get(22));
            double  col23    = Utils.getObjectValueDouble(data.get(23));
            double  col24    = Utils.getObjectValueDouble(data.get(24));
            double  col25    = Utils.getObjectValueInteger(data.get(25));
            double  col26    = Utils.getObjectValueInteger(data.get(26));
            double  col27    = Utils.getObjectValueInteger(data.get(27));
            double  col28    = Utils.getObjectValueDouble(data.get(28));
            double  col29    = Utils.getObjectValueDouble(data.get(29));
            double  col30    = Utils.getObjectValueInteger(data.get(30));
            double  col31    = Utils.getObjectValueDouble( data.get(31));
            row.createCell(0).setCellValue(col0);
            row.createCell(1).setCellValue(col1);
            row.createCell(2).setCellValue(col2);
            row.createCell(3).setCellValue(col3);
            row.createCell(4).setCellValue(col4);
            row.createCell(5).setCellValue(col5);
            row.createCell(6).setCellValue(col6);
            row.createCell(7).setCellValue(col7);
            row.createCell(8).setCellValue(col8);
            row.createCell(9).setCellValue(col9);
            row.createCell(10).setCellValue(col10);
            row.createCell(11).setCellValue(col11);
            row.createCell(12).setCellValue(col12);
            row.createCell(13).setCellValue(col13);
            row.createCell(14).setCellValue(col14);
            row.createCell(15).setCellValue(col15);
            row.createCell(16).setCellValue(col16);
            row.createCell(17).setCellValue(col17);
            row.createCell(18).setCellValue(col18);
            row.createCell(19).setCellValue(col19);
            row.createCell(20).setCellValue(col20);
            row.createCell(21).setCellValue(col21);
            row.createCell(22).setCellValue(col22);
            row.createCell(23).setCellValue(col23);
            row.createCell(24).setCellValue(col24);
            row.createCell(25).setCellValue(col25);
            row.createCell(26).setCellValue(col26);
            row.createCell(27).setCellValue(col27);
            row.createCell(28).setCellValue(col28);
            row.createCell(29).setCellValue(col29);
            row.createCell(30).setCellValue(col30);
            row.createCell(31).setCellValue(col31);


            //设置样式
            for(int i=0;i<32;i++){
                if(i == 7 || i == 8 || i == 10 || i == 11 || i == 24 || i == 25 || i == 26 || i == 27 || i == 28 || i == 29 ){
                    row.getCell(i).setCellStyle(perCentStype);  //设置百分号
                    continue;
                }

//                System.out.println(row.getCell(i));
                row.getCell(i).setCellStyle(borderStyle);
            }
            //设置红色
            if(multiPlanNames.contains(col1)){
                row.getCell(1).setCellStyle(redStyle);
            }

            //处理sum值
            sum5 += col5;
            sum6 += col6;
            sum13 += col13;
            sum14 += col14;
            sum15 += col15;
            sum18 += col18;
            sum19 += col19;
        }
        // 汇总rate7 =G1/F1
        rate7 = sum5 == 0.0 ? 0.0: sum6*1.0 / sum5;
        // rate 12 = =N1/F1*1000
        rate12 = sum5 == 0.0 ? 0.0:sum13 / sum5 * 1000;

        CellStyle sumStyle = workbook.createCellStyle();
        sumStyle.setBorderBottom(BorderStyle.THIN);
        sumStyle.setBorderLeft(BorderStyle.THIN);
        sumStyle.setBorderRight(BorderStyle.THIN);
        sumStyle.setBorderTop(BorderStyle.THIN);
        sumStyle.setVerticalAlignment(VerticalAlignment.CENTER);
        sumStyle.setAlignment(HorizontalAlignment.CENTER);

        CellStyle sumStyle2 = workbook.createCellStyle();  // 带百分号
        sumStyle2.setBorderBottom(BorderStyle.THIN);
        sumStyle2.setBorderLeft(BorderStyle.THIN);
        sumStyle2.setBorderRight(BorderStyle.THIN);
        sumStyle2.setBorderTop(BorderStyle.THIN);
        sumStyle2.setVerticalAlignment(VerticalAlignment.CENTER);
        sumStyle2.setAlignment(HorizontalAlignment.CENTER);
        sumStyle2.setDataFormat((short) BuiltinFormats.getBuiltinFormat("0.00%"));

        //计算求和
        row = sheet.getRow(0);
        row.createCell(5).setCellValue(sum5);
        row.createCell(6).setCellValue(sum6);
        row.createCell(7).setCellValue(rate7);
        row.createCell(12).setCellValue(rate12);
        row.createCell(13).setCellValue(sum13);
        row.createCell(14).setCellValue(sum14);
        row.createCell(15).setCellValue(sum15);
        row.createCell(18).setCellValue(sum18);
        row.createCell(19).setCellValue(sum19);

        //设置样式
        row.getCell(5).setCellStyle(sumStyle);
        row.getCell(6).setCellStyle(sumStyle);
        row.getCell(7).setCellStyle(sumStyle2);
        row.getCell(12).setCellStyle(sumStyle);
        row.getCell(13).setCellStyle(sumStyle);
        row.getCell(14).setCellStyle(sumStyle);
        row.getCell(15).setCellStyle(sumStyle);
        row.getCell(18).setCellStyle(sumStyle2);
        row.getCell(19).setCellStyle(sumStyle2);


        return workbook;
    }

    @Override
    public List<List<Object>> querySql8(List<String> etlDates,List<String> planIds) {
        String sql ="select \n" +
            "    concat(time,'_',plan_id) as token,\n" +
            "    ROUND( SUM( price )/ SUM( `show` )* 1000, 2  ) ecpm \n" +
            "from \n" +
            "     `stat_oppkg` \n" +
            "where \n" +
            "    time in ("+
            etlDates.stream().map(s -> "'"+s+"'").collect(Collectors.joining())
            +")\n" +
            "    AND plan_id IN ( "+ String.join(",",planIds) +" )  -- excel上传的计划\n" +
            "group by \n" +
            "    concat(time,'_',plan_id)";


        System.out.println("sql8:");
        System.out.println(sql);
        System.out.println();
        List<List<Object>> _return = new ArrayList<>();
        JdbcUtils.queryBatch(sql, null, rs ->{
            while(rs.next()){
                int colCnt = rs.getMetaData().getColumnCount();
                List<Object> rsList = new ArrayList<>(colCnt);
                for(int i=1;i<=colCnt;i++){
                    rsList.add(rs.getObject(i));
                }
                _return.add(rsList);
            }
        });
        return _return;
    }

    @Override
    public List<List<Object>> querySql9(List<String> etlDates,int aduserId) {
        String sql ="select \n" +
            "    concat(time,'_',plan_id) as token,\n" +
            "    ROUND( SUM( price )/ SUM( `show` )* 1000, 2  ) ecpm \n" +
            "from \n" +
            "     `stat_oppkg` \n" +
            "where \n" +
            "    time in ("+
            etlDates.stream().map(s -> "'"+s+"'").collect(Collectors.joining())
            +")\n" +
            "    AND aduser_id = "+ aduserId +"  -- excel上传的计划\n" +
            "group by \n" +
            "    concat(time,'_',plan_id)";


        System.out.println("sql9:");
        System.out.println(sql);
        System.out.println();
        List<List<Object>> _return = new ArrayList<>();
        JdbcUtils.queryBatch(sql, null, rs ->{
            while(rs.next()){
                int colCnt = rs.getMetaData().getColumnCount();
                List<Object> rsList = new ArrayList<>(colCnt);
                for(int i=1;i<=colCnt;i++){
                    rsList.add(rs.getObject(i));
                }
                _return.add(rsList);
            }
        });
        return _return;
    }
}
