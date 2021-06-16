package com.vlion.service;

import ml.dmlc.xgboost4j.java.XGBoostError;

/**
 * @description:
 * @author: malichun
 * @time: 2021/4/30/0030 16:56
 */
public interface PredictService {
    public float xgbPredict(float[] data) throws XGBoostError;
}
