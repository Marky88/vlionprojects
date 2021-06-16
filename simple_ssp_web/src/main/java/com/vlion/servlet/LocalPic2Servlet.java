package com.vlion.servlet;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * @description:
 * @author: malichun
 * @time: 2021/4/6/0006 18:33
 */
@WebServlet(name = "picservlet2",
        urlPatterns = "/picservlet2"
)
public class LocalPic2Servlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        //设置编码格式
        request.setCharacterEncoding("UTF-8");
        response.setCharacterEncoding("UTF-8");

        String filePath = request.getParameter("filePath");

        response.setContentType("image/jpeg");

        BufferedInputStream in = new BufferedInputStream(new FileInputStream(filePath));
        byte[] content = new byte[in.available()];

        in.read(content);
        in.close();

        BufferedOutputStream out = new BufferedOutputStream(
                response.getOutputStream());

        out.write(content);
        out.close();

    }
}
