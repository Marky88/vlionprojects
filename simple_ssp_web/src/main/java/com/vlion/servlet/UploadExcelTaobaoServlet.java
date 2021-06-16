package com.vlion.servlet;

import com.vlion.service.ExcelService;
import com.vlion.service.imp.ExcelServiceImp;
import com.vlion.utils.Utils;
import org.apache.poi.ss.usermodel.Workbook;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;

/**
 * @description:
 * @author: malichun
 * @time: 2021/3/23/0023 15:46
 */
@MultipartConfig  //使用MultipartConfig注解标注改servlet能够接受文件上传的请求
public class UploadExcelTaobaoServlet extends HttpServlet {
    ExcelService excelService = new ExcelServiceImp();
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
//        设置编码方式
        req.setCharacterEncoding("utf-8");
        resp.setContentType("text/html;charset=utf-8");
        //        设置输出
        Part part = req.getPart("myfile");
        String disposition = part.getHeader("Content-Disposition");  //应该是个名字
        String filename = disposition.substring(disposition.indexOf("filename=\"")+10,disposition.length()-1);
        String suffix = filename.substring(filename.lastIndexOf("."));
        String fileNameHeader = filename.substring(0,filename.lastIndexOf("."));
//        System.out.println(disposition);
//        System.out.println(fileNameHeader);
        //获取上传的文件名
        InputStream is = part.getInputStream();

        String outFileName = fileNameHeader+"_"+ Utils.getCurrentTime() +suffix;
        String newFilename = Utils.encodeFileName(req, outFileName);
        resp.setContentType("text/x-msdownload");
        resp.setHeader("Content-Disposition", "attachment; filename="+newFilename);

        Workbook workbook = null;
        try {
            workbook = excelService.parseTaobaoExcelService(is, filename);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        //输出
        ServletOutputStream out = resp.getOutputStream();
        workbook.write(out);
        out.close();


    }
}
