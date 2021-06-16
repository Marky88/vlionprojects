package com.vlion.utils;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @description:
 * @author: malichun
 * @time: 2021/3/22/0022 16:53
 */
public interface Process{
    void queryCallBack(ResultSet rs) throws SQLException;
}