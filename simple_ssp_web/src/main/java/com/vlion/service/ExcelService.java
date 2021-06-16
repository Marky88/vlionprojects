package com.vlion.service;

import org.apache.poi.ss.usermodel.Workbook;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;

/**
 * @description:
 * @author: malichun
 * @time: 2021/3/22/0022 15:42
 */
public interface ExcelService {
    //解析Excel,拼接sql结果
    public Workbook parseZhifubaoExcelService(InputStream is, String fileName) throws IOException;

    // 20210604新版支付宝解析
    public Workbook parseZhifubaoExcelService2(InputStream is,String fileName) throws IOException;

    /**
     * 解析上传淘宝的excel
     * @param is
     * @param fileName
     * @return
     * @throws IOException
     */
    public Workbook parseTaobaoExcelService(InputStream is,String fileName) throws IOException, ParseException;

}
