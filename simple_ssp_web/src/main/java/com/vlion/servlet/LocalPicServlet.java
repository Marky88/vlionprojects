package com.vlion.servlet;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;

/**
 * @description:
 * @author: malichun
 * @time: 2021/4/6/0006 17:34
 */

@WebServlet(name = "picservlet",
        urlPatterns = "/picservlet"
)
public class LocalPicServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String path = "E:\\pics\\";//换成自己的
        File folder = new File(path);
        File temp[] = folder.listFiles();
        String[] picNames = new String[temp.length];
        for(int i=0;i<temp.length;i++) {
            picNames[i] = temp[i].getName();
        }
        req.setAttribute("picNames", picNames);
        req.setCharacterEncoding("utf-8");
        req.getRequestDispatcher("/pic.jsp").forward(req,resp);

    }
}
