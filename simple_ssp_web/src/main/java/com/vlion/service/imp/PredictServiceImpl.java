package com.vlion.service.imp;

import com.vlion.service.PredictService;
import ml.dmlc.xgboost4j.java.Booster;
import ml.dmlc.xgboost4j.java.DMatrix;
import ml.dmlc.xgboost4j.java.XGBoost;
import ml.dmlc.xgboost4j.java.XGBoostError;

/**
 * @description:
 * @author: malichun
 * @time: 2021/4/30/0030 16:57
 */
public class PredictServiceImpl implements PredictService {
    @Override
    public float xgbPredict(float[] data) throws XGBoostError {
        int nrow = 1;
        int ncol = 4;
        float missing = 0.0f;
        DMatrix dmat = new DMatrix(data, nrow, ncol, missing);
        Booster booster = XGBoost.loadModel("E:\\gitdir\\learn_projects\\myLearn\\simple_ssp_web\\src\\main\\resources\\out.pkl");
//        Booster booster = XGBoost.loadModel("src/main/resources/xgb_model.pkl_2");
        float[][] predicts = booster.predict(dmat);
        float res = 0.0f;
        for (float[] array : predicts) {
            for (float values : array) {
                res = values;
                return res;
            }
        }
        return res;
    }
}
