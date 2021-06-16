package com.vlion.servlet;

import com.alibaba.fastjson.JSONObject;
import com.vlion.service.PredictService;
import com.vlion.service.imp.PredictServiceImpl;
import ml.dmlc.xgboost4j.java.XGBoostError;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.List;

/**
 * @description:
 * @author: malichun
 * @time: 2021/4/30/0030 16:48
 */
public class QueryPredictServlet extends HttpServlet {
    PredictService predictService = new PredictServiceImpl();

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        request.setCharacterEncoding("UTF-8");
        response.setCharacterEncoding("UTF-8");
        response.setContentType("text/html; charset=UTF-8");
        response.setHeader("Content-Type", "text/html; charset=UTF-8");

        BufferedReader br = new BufferedReader(new InputStreamReader((ServletInputStream) request.getInputStream()));
        String line = null;
        StringBuilder sb = new StringBuilder();

        while ((line = br.readLine()) != null) {
            sb.append(line);
        }

        List<Float> floats = JSONObject.parseArray(sb.toString(), Float.class);
        float[] datas = new float[floats.size()];
        for(int i=0;i<floats.size();i++){
            datas[i] = floats.get(i);
        }

        PrintWriter writer=response.getWriter();

        float res = 0.0f;
        try {
            res = predictService.xgbPredict(datas);
        } catch (XGBoostError xgBoostError) {
            xgBoostError.printStackTrace();
        }
        writer.println(res);




    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        System.out.println("aaaaaaaaa");
    }
}
