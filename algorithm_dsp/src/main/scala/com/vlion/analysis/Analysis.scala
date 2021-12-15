package com.vlion.analysis


import java.text.SimpleDateFormat
import java.util.Calendar

import com.vlion.udfs.{MaxCountColUDAF, ParseBrandUDF, TimeUDAF, UserAgentUDF}
import org.apache.spark.sql.SparkSession

/**
 * @description:
 * @author:
 * @time: 2021/12/9/0009 13:23
 *
 */
object Analysis {

    def summaryDay(spark:SparkSession,etl_date :String,bundleArr:Array[String])= {
        val calculateday = 1   //需计算3天
        val summayDayTable = "behavior.behavior_summary_day_dsp"

        spark.udf.register("max_count_col", new MaxCountColUDAF)
        spark.udf.register("parse_brand", new ParseBrandUDF().parseBrand _)
        spark.udf.register("urlDecode", UserAgentUDF.decodeURL _)
        spark.udf.register("convert_ua", UserAgentUDF.convertUa _)
        spark.udf.register("time_process", new TimeUDAF(etl_date))

        summaryOneDay(spark, etl_date, bundleArr)
        summaryCalculateDay(spark, etl_date, bundleArr,calculateday)

        //写入到hive表
        spark.sql(
            s"""
               |insert overwrite table ${summayDayTable} partition (etl_date = '${etl_date}')
               |select
               |    t1.if_imp,
               |    t1.if_clk,
               |    t1.device_id,
               |    t1.pkg_name as bundle,
               |    t1.advlocation_id,
               |    t1.os,
               |    t1.producer,
               |    t1.phone_model,
               |    t1.osv,
               |    t1.brand,
               |    t1.screen_width,
               |    t1.screen_height,
               |    t1.carrier,
               |    t1.network,
               |    t1.country_code,
               |    t1.continent,
               |    t1.city,
               |    t1.log_hour,
               |    t1.longitude,
               |    t1.latitude,
               |    t1.ua,
               |    t1.adsolt_type,
               |    t1.adsolt_width,
               |    t1.adsolt_height,
               |    t1.user_id,
               |    t1.user_birth_year,
               |    t1.user_sex,
               |    t1.hobby_label,
               |    t1.req_count,  --竞价请求次数
               |    t1.req_rate,   --竞价请求占比
               |    t1.req_avg_real_price, --竞价请求平均价格
               |    t1.req_max_real_price, --竞价请求最高价格
               |    t1.req_min_real_price,  --竞价请求最低价格
               |    t1.imp_count, --曝光次数
               |    t1.imp_rate,  --曝光次数占比
               |    t1.imp_creative_count,  --曝光创意种类次数
               |    t1.imp_max_count_creative, --曝光最多的创意
               |    t1.imp_avg_real_price,  --曝光平均价格
               |    t1.imp_max_real_price,  --曝光最高价格
               |    t1.imp_min_real_price,  --曝光最低价格
               |    t1.clk_count,  --点击次数
               |    t1.clk_rate,  -- 点击次数占比
               |    t1.clk_creative_count, -- 点击创意种类
               |    t1.clk_max_creative,  -- 点击最多的创意
               |    t1.clk_first_seconds, -- 第一次在本时间段内的秒数
               |    t1.clk_last_seconds, --最后一次在本时间段内的秒数
               |    t1.clk_min_interval, --点击间隔最小时间
               |    t1.clk_max_interval, -- 点击间隔最大时间
               |    t1.clk_avg_interval, -- 点击间隔平均时间
               |    t2.req_count_3,  --竞价请求次数   --下面表示前3天开始的统计
               |    t2.req_rate_3,   --竞价请求占比
               |    t2.req_avg_real_price_3, --竞价请求平均价格
               |    t2.req_max_real_price_3, --竞价请求最高价格
               |    t2.req_min_real_price_3,  --竞价请求最低价格
               |    t2.imp_count_3, --曝光次数
               |    t2.imp_rate_3,  --曝光次数占比
               |    t2.imp_creative_count_3,  --曝光创意种类次数
               |    t2.imp_max_count_creative_3, --曝光最多的创意
               |    t2.imp_avg_real_price_3,  --曝光平均价格
               |    t2.imp_max_real_price_3,  --曝光最高价格
               |    t2.imp_min_real_price_3,  --曝光最低价格
               |    t2.clk_count_3,  --点击次数
               |    t2.clk_rate_3,  -- 点击次数占比
               |    t2.clk_creative_count_3, -- 点击创意种类
               |    t2.clk_max_creative_3  -- 点击最多的创意
               |from
               |    summary_one_day  t1
               |left join
               |    summary_Cal_Day  t2
               |on  t1.device_id = t2.device_id
               |  and t1.pkg_name = t2.pkg_name
               |  and t1.advlocation_id = t2.advlocation_id
               |""".stripMargin
        )

/*        frame.show(2)

        frame.createOrReplaceTempView("tmp")

        spark.sql(
            s"""
               |insert overwrite table ${summayDayTable} partition (etl_date = '${etl_date}')
               |select
               | *
               |from
               |  tmp
               |
               |""".stripMargin
        )*/


    }

        def summaryOneDay(spark:SparkSession,etl_date: String,bundleArr: Array[String]):Unit = {
            val bundles = bundleArr.map(x => "'" + x + "'").mkString(",")
            //bundle.map(println(_))

             spark.sql(
                s"""
                   |select
                   |    t1.device_id,
                   |    t1.pkg_name,
                   |    t1.advlocation_id,
                   |    t1.os,
                   |    t1.producer,
                   |    t1.phone_model,
                   |    t1.osv,
                   |    t1.brand,
                   |    t1.screen_width,
                   |    t1.screen_height,
                   |    t1.carrier,
                   |    t1.network,
                   |    t1.country_code,
                   |    t1.continent,
                   |    t1.city,
                   |    t1.log_hour,
                   |    t1.longitude,
                   |    t1.latitude,
                   |    t1.ua,
                   |    t1.adsolt_type,
                   |    t1.adsolt_width,
                   |    t1.adsolt_height,
                   |    t1.user_id,
                   |    t1.user_birth_year,
                   |    t1.user_sex,
                   |    t1.hobby_label,
                   |    t1.req_count,  --竞价请求次数
                   |    t1.req_rate,   --竞价请求占比
                   |    t1.avg_real_price as req_avg_real_price, --竞价请求平均价格
                   |    t1.max_real_price as req_max_real_price, --竞价请求最高价格
                   |    t1.min_real_price as req_min_real_price,  --竞价请求最低价格
                   |    t2.imp_count, --曝光次数
                   |    t2.imp_rate,  --曝光次数占比
                   |    t2.creative_count as imp_creative_count,  --曝光创意种类次数
                   |    t2.max_count_creative as imp_max_count_creative, --曝光最多的创意
                   |    t2.avg_real_price as imp_avg_real_price,  --曝光平均价格
                   |    t2.max_real_price as imp_max_real_price,  --曝光最高价格
                   |    t2.min_real_price as imp_min_real_price,  --曝光最低价格
                   |    t3.clk_count,  --点击次数
                   |    t3.clk_rate,  -- 点击次数占比
                   |    t3.creative_count as clk_creative_count, -- 点击创意种类
                   |    t3.max_creative as clk_max_creative,  -- 点击最多的创意
                   |    t3.first_seconds as clk_first_seconds, -- 第一次在本时间段内的秒数
                   |    t3.last_seconds as clk_last_seconds, --最后一次在本时间段内的秒数
                   |    t3.min_interval as clk_min_interval, --点击间隔最小时间
                   |    t3.max_interval as clk_max_interval, -- 点击间隔最大时间
                   |    t3.avg_interval as clk_avg_interval, -- 点击间隔平均时间
                   |    if (t2.imp_count >= 1, 1, 0) as if_imp,
                   |    if (t3.clk_count >= 1, 1, 0) as if_clk
                   |from
                   |(--竞价日志
                   |    select
                   |       device_id,
                   |       pkg_name,
                   |       advlocation_id,
                   |       os,
                   |       producer,
                   |       phone_model,
                   |       osv,
                   |       brand,
                   |       screen_width,
                   |       screen_height,
                   |       carrier,
                   |       network,
                   |       country_code,
                   |       continent,
                   |       city,
                   |       log_hour,
                   |       floor(longitude) as longitude,
                   |       floor(latitude) as latitude,
                   |       ua,
                   |       adsolt_type,
                   |       adsolt_width,
                   |       adsolt_height,
                   |       user_id,
                   |       user_birth_year,
                   |       user_sex,
                   |       hobby_label,
                   |       req_count,
                   |       req_count/ sum(req_count) over (partition by device_id) as req_rate,
                   |       avg_real_price,
                   |       max_real_price,
                   |       min_real_price
                   |    from
                   |    (
                   |        select
                   |           device_id,
                   |           pkg_name,
                   |           advlocation_id,
                   |           max(os) as os,
                   |           max_count_col(producer) as producer,
                   |           max_count_col(phone_model) as phone_model,
                   |           max(osv) as osv,
                   |           max_count_col((parse_brand(producer))) as brand,
                   |           max(screen_width) as screen_width,
                   |           max(screen_height) as screen_height,
                   |           max_count_col(carrier) as carrier,
                   |           max_count_col(network) as network,
                   |           max_count_col(country_code) as country_code,
                   |           max_count_col(continent) as continent,
                   |           max_count_col(city) as city,
                   |           max_count_col(log_hour) as log_hour,
                   |           max(longitude) as longitude,
                   |           max(latitude) as latitude,
                   |           max(urlDecode(user_agent)) as ua,
                   |           max(adsolt_type) as adsolt_type,
                   |           max(adsolt_width) as adsolt_width,
                   |           max(adsolt_height) as adsolt_height,
                   |           max_count_col(user_id) as user_id,
                   |           max_count_col(user_birth_year) as user_birth_year,
                   |           max_count_col(user_sex) as user_sex,
                   |           max_count_col(hobby_label) as hobby_label,
                   |           count(1) as req_count,  --请求次数
                   |           -- 竞价请求次数占比
                   |           avg(floor_price) as avg_real_price,  --竞价请求平均价格
                   |           max(floor_price) as max_real_price,  --竞价请求最高价格
                   |           min(floor_price) as min_real_price   --竞价请求最低价格
                   |        from
                   |            ods.ods_dsp_info
                   |        where logtype = '10'
                   |            and etl_date = '${etl_date}'
                   |            and time is not null and time != ''
                   |            and pkg_name in (${bundles})
                   |        group by device_id,pkg_name,advlocation_id
                   |    ) t
                   |)t1
                   |left join
                   |(--曝光日志
                   |   select
                   |       device_id,
                   |       pkg_name,
                   |       advlocation_id,
                   |       imp_count,
                   |       imp_count/ sum(imp_count) over (partition by device_id) as imp_rate,
                   |       creative_count,
                   |       max_count_creative,
                   |       avg_real_price,
                   |       max_real_price,
                   |       min_real_price
                   |   from
                   |   (
                   |       select
                   |           device_id,
                   |           pkg_name,
                   |           advlocation_id,
                   |           count(1) as imp_count,
                   |           --曝光次数占比
                   |           count(distinct creative_id) as creative_count,
                   |           max_count_col(creative_id) as max_count_creative,
                   |           avg(real_price) as avg_real_price,
                   |           max(real_price) as max_real_price,
                   |           min(real_price) as min_real_price
                   |       from
                   |          ods.ods_dsp_info
                   |       where logtype = '14'
                   |              and etl_date = '${etl_date}'
                   |              and time is not null and time != ''
                   |              and pkg_name  in (${bundles})
                   |       group by device_id,pkg_name,advlocation_id
                   |   ) t
                   |) t2
                   |on  t1.device_id = t2.device_id and t1.pkg_name = t2.pkg_name and t1.advlocation_id = t2.advlocation_id
                   |left join
                   |( --点击日志
                   |  select
                   |    device_id,
                   |    pkg_name,
                   |    advlocation_id,
                   |    clk_count,
                   |    clk_count/ sum(clk_count) over (partition by device_id) as clk_rate,  -- 点击次数占比
                   |    creative_count, -- 点击创意种类
                   |    max_creative,  -- 点击最多的创意
                   |    time_elements[0] as first_seconds, -- 第一次在本时间段内的秒数
                   |    time_elements[1] as last_seconds, --最后一次在本时间段内的秒数
                   |    time_elements[2] as min_interval, --点击间隔最小时间
                   |    time_elements[3] as max_interval, -- 点击间隔最大时间
                   |    time_elements[4] as avg_interval -- 点击间隔平均时间
                   |  from
                   |    (
                   |       select
                   |         device_id,
                   |         pkg_name,
                   |         advlocation_id,
                   |         count(1) as clk_count,
                   |         --点击次数占比
                   |         count(distinct creative_id) as creative_count,
                   |         max_count_col(creative_id) as max_creative,
                   |         time_process(time) as time_elements
                   |       from
                   |         ods.ods_dsp_info
                   |       where logtype = '15'
                   |         and etl_date = '${etl_date}'
                   |         and time is not null and time != ''
                   |         and pkg_name  in (${bundles})
                   |       group by device_id,pkg_name,advlocation_id
                   |    ) t
                   |)t3
                   |on t1.device_id = t3.device_id and t1.pkg_name = t3.pkg_name and t1.advlocation_id = t3.advlocation_id
                   |
                   |""".stripMargin
            ).createOrReplaceTempView("summary_one_day")

            //frame.show(1)
          //  frame.coalesce(1).write.option("header","true").csv("/test/demo/input/b.csv")

        }




        def summaryCalculateDay(spark:SparkSession,etl_date: String,bundleArr: Array[String],calDay:Int):Unit={
            val bundles = bundleArr.map(x => "'" + x + "'").mkString(",")
            //日期转换
            val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
            val date = dateFormat.parse(etl_date)
            val calendar = Calendar.getInstance()
            calendar.setTime(date)
            calendar.add(Calendar.DATE,-calDay)
            val headDay = dateFormat.format(calendar.getTime())



           spark.sql(
                s"""
                   |select
                   |    t1.device_id,
                   |    t1.pkg_name,
                   |    t1.advlocation_id,
                   |    t1.req_count as req_count_3,  --竞价请求次数
                   |    t1.req_rate as req_rate_3,   --竞价请求占比
                   |    t1.avg_real_price as req_avg_real_price_3, --竞价请求平均价格
                   |    t1.max_real_price as req_max_real_price_3, --竞价请求最高价格
                   |    t1.min_real_price as req_min_real_price_3,  --竞价请求最低价格
                   |    t2.imp_count as imp_count_3, --曝光次数
                   |    t2.imp_rate as imp_rate_3,  --曝光次数占比
                   |    t2.creative_count as imp_creative_count_3,  --曝光创意种类次数
                   |    t2.max_count_creative as imp_max_count_creative_3, --曝光最多的创意
                   |    t2.avg_real_price as imp_avg_real_price_3,  --曝光平均价格
                   |    t2.max_real_price as imp_max_real_price_3,  --曝光最高价格
                   |    t2.min_real_price as imp_min_real_price_3,  --曝光最低价格
                   |    t3.clk_count as clk_count_3,  --点击次数
                   |    t3.clk_rate as clk_rate_3,  -- 点击次数占比
                   |    t3.creative_count as clk_creative_count_3, -- 点击创意种类
                   |    t3.max_creative as clk_max_creative_3  -- 点击最多的创意
                   |from
                   |(--竞价日志
                   |    select
                   |       device_id,
                   |       pkg_name,
                   |       advlocation_id,
                   |       os,
                   |       producer,
                   |       phone_model,
                   |       osv,
                   |       brand,
                   |       screen_width,
                   |       screen_height,
                   |       carrier,
                   |       network,
                   |       country_code,
                   |       continent,
                   |       city,
                   |       log_hour,
                   |       floor(longitude) as longitude,
                   |       floor(latitude) as latitude,
                   |       ua,
                   |       adsolt_type,
                   |       adsolt_width,
                   |       adsolt_height,
                   |       user_id,
                   |       user_birth_year,
                   |       user_sex,
                   |       hobby_label,
                   |       req_count,
                   |       req_count/ sum(req_count) over (partition by device_id) as req_rate,
                   |       avg_real_price,
                   |       max_real_price,
                   |       min_real_price
                   |    from
                   |    (
                   |        select
                   |           device_id,
                   |           pkg_name,
                   |           advlocation_id,
                   |           max(os) as os,
                   |           max_count_col(producer) as producer,
                   |           max_count_col(phone_model) as phone_model,
                   |           max(osv) as osv,
                   |           max_count_col((parse_brand(producer))) as brand,
                   |           max(screen_width) as screen_width,
                   |           max(screen_height) as screen_height,
                   |           max_count_col(carrier) as carrier,
                   |           max_count_col(network) as network,
                   |           max_count_col(country_code) as country_code,
                   |           max_count_col(continent) as continent,
                   |           max_count_col(city) as city,
                   |           max_count_col(log_hour) as log_hour,
                   |           max(longitude) as longitude,
                   |           max(latitude) as latitude,
                   |           max(urlDecode(user_agent)) as ua,
                   |           max(adsolt_type) as adsolt_type,
                   |           max(adsolt_width) as adsolt_width,
                   |           max(adsolt_height) as adsolt_height,
                   |           max_count_col(user_id) as user_id,
                   |           max_count_col(user_birth_year) as user_birth_year,
                   |           max_count_col(user_sex) as user_sex,
                   |           max_count_col(hobby_label) as hobby_label,
                   |           count(1) as req_count,  --请求次数
                   |           -- 竞价请求次数占比
                   |           avg(floor_price) as avg_real_price,  --竞价请求平均价格
                   |           max(floor_price) as max_real_price,  --竞价请求最高价格
                   |           min(floor_price) as min_real_price   --竞价请求最低价格
                   |        from
                   |            ods.ods_dsp_info
                   |        where logtype = '10'
                   |            and etl_date > '${headDay}' and etl_date <= '${etl_date}'
                   |            and time is not null and time != ''
                   |            and pkg_name in (${bundles})
                   |        group by device_id,pkg_name,advlocation_id
                   |    ) t
                   |)t1
                   |left join
                   |(--曝光日志
                   |   select
                   |       device_id,
                   |       pkg_name,
                   |       advlocation_id,
                   |       imp_count,
                   |       imp_count/ sum(imp_count) over (partition by device_id) as imp_rate,
                   |       creative_count,
                   |       max_count_creative,
                   |       avg_real_price,
                   |       max_real_price,
                   |       min_real_price
                   |   from
                   |   (
                   |       select
                   |           device_id,
                   |           pkg_name,
                   |           advlocation_id,
                   |           count(1) as imp_count,
                   |           --曝光次数占比
                   |           count(distinct creative_id) as creative_count,
                   |           max_count_col(creative_id) as max_count_creative,
                   |           avg(real_price) as avg_real_price,
                   |           max(real_price) as max_real_price,
                   |           min(real_price) as min_real_price
                   |       from
                   |          ods.ods_dsp_info
                   |       where logtype = '14'
                   |              and etl_date > '${headDay}' and etl_date <= '${etl_date}'
                   |              and time is not null and time != ''
                   |              and pkg_name  in (${bundles})
                   |       group by device_id,pkg_name,advlocation_id
                   |   ) t
                   |) t2
                   |on  t1.device_id = t2.device_id and t1.pkg_name = t2.pkg_name and t1.advlocation_id = t2.advlocation_id
                   |left join
                   |( --点击日志
                   |  select
                   |    device_id,
                   |    pkg_name,
                   |    advlocation_id,
                   |    clk_count,
                   |    clk_count/ sum(clk_count) over (partition by device_id) as clk_rate,  -- 点击次数占比
                   |    creative_count, -- 点击创意种类
                   |    max_creative,  -- 点击最多的创意
                   |    time_elements[0] as first_seconds, -- 第一次在本时间段内的秒数
                   |    time_elements[1] as last_seconds, --最后一次在本时间段内的秒数
                   |    time_elements[2] as min_interval, --点击间隔最小时间
                   |    time_elements[3] as max_interval, -- 点击间隔最大时间
                   |    time_elements[4] as avg_interval -- 点击间隔平均时间
                   |  from
                   |    (
                   |       select
                   |         device_id,
                   |         pkg_name,
                   |         advlocation_id,
                   |         count(1) as clk_count,
                   |         --点击次数占比
                   |         count(distinct creative_id) as creative_count,
                   |         max_count_col(creative_id) as max_creative,
                   |         time_process(time) as time_elements
                   |       from
                   |         ods.ods_dsp_info
                   |       where logtype = '15'
                   |         and etl_date > '${headDay}' and etl_date <= '${etl_date}'
                   |         and time is not null and time != ''
                   |         and pkg_name  in (${bundles})
                   |       group by device_id,pkg_name,advlocation_id
                   |    ) t
                   |)t3
                   |on t1.device_id = t3.device_id and t1.pkg_name = t3.pkg_name and t1.advlocation_id = t3.advlocation_id
                   |
                   |""".stripMargin
            ).createOrReplaceTempView("summary_Cal_Day")



    }




}
