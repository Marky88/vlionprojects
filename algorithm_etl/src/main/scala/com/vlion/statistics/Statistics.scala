package com.vlion.statistics

import java.text.SimpleDateFormat
import java.util.Date

import com.vlion.udfs.{MaxCountColUDAF, ParseBrandUDF, TimeUDAF, UserAgentUDF}
import com.vlion.util.Constant
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * @description:
 * @author: malichun
 * @time: 2021/7/23/0023 16:17
 *
 */
object Statistics {

//    val appIds = Array("30858","31433","30804")

    def summaryDay(spark: SparkSession, etlDate: String): Unit = {
        spark.udf.register("max_count_col", new MaxCountColUDAF)
        spark.udf.register("time_process", new TimeUDAF(etlDate))
        spark.udf.register("parse_brand", new ParseBrandUDF().parseBrand _ )
        spark.udf.register("convert_ua",UserAgentUDF.convertUa _)

        summaryDayWithMedia(spark,etlDate,Constant.appIds)
        summaryDayNoMedia(spark,etlDate,Constant.appIds)

        val calDays = 3
        summaryCalculateDay(spark,etlDate,calDays,Constant.appIds)

        mediaLinkOcpx(spark,etlDate,Constant.appIds)

//        val targetTable ="behavior.behavior_summary_day_20210816" //

        val targetTable = "behavior.behavior_summary_day_20210825"  // 添加3天的数据量

        //需要的
       spark.sql(
            raw"""
                 |insert overwrite table $targetTable partition(etl_date = '${etlDate}')
                 |select
                 |    t1.if_imp,
                 |    t1.if_clk,
                 |    t1.app_id,
                 |    t1.id,
                 |    t1.adsolt_id,
                 |    t1.id_type,
                 |    t1.os,
                 |    t1.producer,
                 |    t1.model,
                 |    t1.osv,
                 |    t1.brand,
                 |    t1.adsolt_width,
                 |    t1.adsolt_height,
                 |    t1.carrier,
                 |    t1.network,
                 |    t1.city_id,
                 |    t1.pkg_name,
                 |    t1.adsolt_type,
                 |    t1.real_adsolt_width,
                 |    t1.real_adsolt_height,
                 |    t1.hour,
                 |    if(t1.longitude like '%.%',split(t1.longitude,'\\.')[0],t1.longitude) as longitude,
                 |    if(t1.latitude like '%.%',split(t1.latitude,'\\.')[0],t1.latitude) as latitude,
                 |    convert_ua(t1.ua) as ua,  -- 转换后的ua
                 |    t1.req_count,
                 |    t1.req_rate, -- 竞价请求次数占比
                 |    t1.req_avg_real_price, -- 竞价请求平均价格
                 |    t1.req_max_real_price, -- 竞价请求最高价格
                 |    t1.req_min_real_price,
                 |    t1.imp_cnt , -- 曝光次数
                 |    t1.imp_rate, -- 曝光次数占比
                 |    t1.imp_creative_count, -- 曝光创意种类次数
                 |    t1.imp_max_count_creative,  -- 曝光最多的创意
                 |    t1.imp_avg_real_price, -- 曝光平均价格
                 |    t1.imp_max_real_price,
                 |    t1.imp_min_real_price, -- 曝光最低价格
                 |    t1.clk_cnt,
                 |    t1.clk_rate,
                 |    t1.clk_creative_count, -- 点击创意种类次数
                 |    t1.clk_max_count_creative,  -- 点击最多的创意
                 |    t1.clk_first_seconds,
                 |    t1.clk_last_seconds,
                 |    t1.clk_min_interval,
                 |    t1.clk_max_interval,
                 |    t1.clk_avg_interval,
                 |    t2.req_count as req_count_2,
                 |    t2.req_rate as req_rate_2, -- 竞价请求次数占比
                 |    t2.req_avg_real_price as req_avg_real_price_2, -- 竞价请求平均价格
                 |    t2.req_max_real_price as req_max_real_price_2, -- 竞价请求最高价格
                 |    t2.req_min_real_price as req_min_real_price_2,
                 |    t2.req_app_count as req_app_count_2,
                 |    t2.imp_cnt as imp_cnt_2, -- 曝光次数
                 |    t2.imp_rate as imp_rate_2, -- 曝光次数占比
                 |    t2.imp_creative_count as imp_creative_count_2, -- 曝光创意种类次数
                 |    t2.imp_max_count_creative as imp_max_count_creative_2,  -- 曝光最多的创意
                 |    t2.imp_avg_real_price as imp_avg_real_price_2, -- 曝光平均价格
                 |    t2.imp_max_real_price as imp_max_real_price_2,
                 |    t2.imp_min_real_price as imp_min_real_price_2, -- 曝光最低价格
                 |    t2.imp_app_count as imp_app_count_2,
                 |    t2.clk_cnt as clk_cnt_2,
                 |    t2.clk_rate as clk_rate_2,
                 |    t2.clk_creative_count as clk_creative_count_2, -- 点击创意种类次数
                 |    t2.clk_max_count_creative as clk_max_count_creative_2,  -- 点击最多的创意
                 |    t2.clk_first_seconds as clk_first_seconds_2,
                 |    t2.clk_last_seconds as clk_last_seconds_2,
                 |    t2.clk_min_interval as clk_min_interval_2,
                 |    t2.clk_max_interval as clk_max_interval_2,
                 |    t2.clk_avg_interval as clk_avg_interval_2,
                 |    t2.clk_app_count as clk_app_count_2,
                 |    t3.req_count as req_count_3,
                 |    t3.req_rate as req_rate_3, -- 竞价请求次数占比
                 |    t3.req_avg_real_price as req_avg_real_price_3, -- 竞价请求平均价格
                 |    t3.req_max_real_price as req_max_real_price_3, -- 竞价请求最高价格
                 |    t3.req_min_real_price as req_min_real_price_3,
                 |    t3.imp_cnt as imp_cnt_3, -- 曝光次数
                 |    t3.imp_rate as imp_rate_3, -- 曝光次数占比
                 |    t3.imp_creative_count as imp_creative_count_3, -- 曝光创意种类次数
                 |    t3.imp_max_count_creative as imp_max_count_creative_3,  -- 曝光最多的创意
                 |    t3.imp_avg_real_price as imp_avg_real_price_3, -- 曝光平均价格
                 |    t3.imp_max_real_price as imp_max_real_price_3,
                 |    t3.imp_min_real_price as imp_min_real_price_3, -- 曝光最低价格
                 |    t3.clk_cnt as clk_cnt_3,
                 |    t3.clk_rate as clk_rate_3,
                 |    t3.clk_creative_count as clk_creative_count_3, -- 点击创意种类次数
                 |    t3.clk_max_count_creative as clk_max_count_creative_3,  -- 点击最多的创意
                 |    t4.plan_id,       --计划id
                 |    t4.create_id,     --创意id
                 |    t4.admaster_id,   --广告主id
                 |    t4.ocpx_tag,      --ocpx转换类型
                 |    t4.if_new_enter,  --是否新登
                 |    t4.if_live_awaken,  --是否拉活唤醒
                 |    t4.if_pay,     --是否支付
                 |    t4.if_mau      --是否MAU
                 |from
                 |    behavior_day_with_media t1
                 |left join
                 |    behavior_day_no_media t2
                 |on t1.id = t2.id
                 |left join
                 |    behavior_with_media_D3 t3
                 |on t1.id= t3.id and t1.adsolt_id = t3.adsolt_id and t1.app_id = t3.app_id
                 |left join
                 |    behavior_media_with_ocpx t4
                 |on t1.id= t4.id and t1.adsolt_id = t4.adsolt_id and t1.app_id = t4.app_id
                 |""".stripMargin)
    }

    private def summaryDayWithMedia(spark: SparkSession, etlDate: String,appIdArray:Array[String]): Unit = {

        val appIdsStr: String = appIdArray.map(x => "'"+x+"'").mkString(",")
        appIdsStr.foreach(print)

        spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        spark.sql("set hive.exec.dynamic.partition =true")

        // 分媒体,广告位id
        spark.sql(
            s"""
               |
               |select
               |    t1.app_id,
               |    t1.id,
               |    t1.adsolt_id,
               |    t1.id_type,
               |    t1.os,
               |    t1.producer,
               |    t1.model,
               |    t1.osv,
               |    t1.brand,
               |    t1.adsolt_width,
               |    t1.adsolt_height,
               |    t1.carrier,
               |    t1.network,
               |    t1.city_id,
               |    t1.pkg_name,
               |    t1.adsolt_type,
               |    t1.real_adsolt_width,
               |    t1.real_adsolt_height,
               |    t1.hour,
               |    t1.longitude,
               |    t1.latitude,
               |    t1.ua,
               |    t1.req_count,
               |    t1.req_rate, -- 竞价请求次数占比
               |    t1.avg_real_price as req_avg_real_price, -- 竞价请求平均价格
               |    t1.max_real_price as req_max_real_price, -- 竞价请求最高价格
               |    t1.min_real_price as req_min_real_price,
               |    t2.imp_cnt , -- 曝光次数
               |    t2.imp_rate, -- 曝光次数占比
               |    t2.creative_count as imp_creative_count, -- 曝光创意种类次数
               |    t2.max_count_creative as imp_max_count_creative,  -- 曝光最多的创意
               |    t2.avg_real_price as imp_avg_real_price, -- 曝光平均价格
               |    t2.max_real_price as imp_max_real_price,
               |    t2.min_real_price as imp_min_real_price, -- 曝光最低价格
               |    t3.clk_cnt,
               |    t3.clk_rate,
               |    t3.creative_count as clk_creative_count, -- 点击创意种类次数
               |    t3.max_count_creative as clk_max_count_creative,  -- 点击最多的创意
               |    t3.first_seconds as clk_first_seconds,
               |    t3.last_seconds as clk_last_seconds,
               |    t3.min_interval as clk_min_interval,
               |    t3.max_interval as clk_max_interval,
               |    t3.avg_interval as clk_avg_interval,
               |    t1.etl_date,
               |    if(t2.imp_cnt >= 1,1,0) as if_imp,
               |    if(t3.clk_cnt >= 1,1,0) as if_clk
               |from
               |( -- 媒体请求
               |    select
               |        app_id,
               |        id,
               |        adsolt_id,
               |        id_type,
               |        os,
               |        producer,
               |        model,
               |        osv,
               |        brand,
               |        adsolt_width,
               |        adsolt_height,
               |        carrier,
               |        network,
               |        city_id,
               |        pkg_name,
               |        adsolt_type,
               |        real_adsolt_width,
               |        real_adsolt_height,
               |        etl_date,
               |        req_count,
               |        req_count/ sum(req_count) over(partition by id ) req_rate,  -- 竞价请求次数占比
               |        avg_real_price,
               |        max_real_price,
               |        min_real_price,
               |        hour,
               |        longitude,
               |        latitude,
               |        ua
               |    from
               |    (
               |        select
               |            app_id,
               |            id,
               |            adsolt_id,
               |            max(id_type) as id_type, -- 设备id类型
               |            max(os) as os,
               |            max_count_col(producer) as producer,
               |            max(model) as model,
               |            max(osv) as osv,
               |            max(brand) as brand,
               |            max(adsolt_width) as adsolt_width,
               |            max(adsolt_height) as adsolt_height,
               |            max_count_col(carrier) as carrier,-- 运营商,取次数最多的一条
               |            max_count_col(network) as network,
               |            max_count_col(city_id) as city_id,
               |            max(pkg_name) as pkg_name,
               |            max(adsolt_type) as adsolt_type,
               |            max(real_adsolt_width) as real_adsolt_width,
               |            max(real_adsolt_height) as real_adsolt_height,
               |            max(etl_date) as etl_date,
               |            sum(req_count) as req_count, -- 竞价请求次数
               |            -- 竞价请求次数占比
               |            avg(floor_price) as avg_real_price,  -- 竞价请求平均价格
               |            max(floor_price) as max_real_price,-- 竞价请求最高价格
               |            min(floor_price) as min_real_price,-- 竞价请求最低价格
               |            max(etl_hour) as hour,
               |            max(longitude) as longitude,
               |            max(latitude) as latitude,
               |            max(user_agent) as ua
               |        from
               |        (
               |            select
               |                case when os='1' then
               |                    case when not ( imei is null or imei ='' or imei = '000000000000000') then if(length(imei) = 32,imei,md5(imei))
               |                         when oaid is not null and oaid != '' and oaid !='00000000-0000-0000-0000-000000000000' then if(length(oaid) = 32,oaid,md5(oaid))
               |                         else null end
               |
               |                    when os ='2' then if(idfa is not null and idfa != '', if(length(idfa)=32,idfa,md5(idfa)) ,null)
               |                    else null end
               |                    as id,
               |                app_id, --媒体id
               |                adsolt_id, -- 广告位id,
               |                if(os='1',if( imei is null or imei ='' or imei = '000000000000000',3,1),2) as id_type, -- 设备id类型, imei(1)/idfa(2)/oaid(3)
               |                os,
               |                producer, -- 制造商,出现次数最高的制造商
               |                model, -- 手机型号
               |                osv, -- 系统版本
               |                parse_brand(producer) as brand, -- udf过滤后的品牌
               |                adsolt_width, -- 屏幕宽
               |                adsolt_height, -- 屏幕高
               |                carrier, -- 运营商
               |                network, -- 网络连接类型
               |                city_id, -- 地域
               |                pkg_name,--, 包名..............................................
               |                adsolt_type, -- 广告位类型
               |                if(real_adsolt_shape like '%x%',split(real_adsolt_shape,'x')[0],null) as real_adsolt_width,-- 广告位宽
               |                if(real_adsolt_shape like '%x%',split(real_adsolt_shape,'x')[1],null) as real_adsolt_height,-- 广告位高
               |                etl_date, -- 日期
               |                1 as req_count, -- 竞价请求次数
               |                floor_price, -- 竞价请求价格
               |                etl_hour,
               |                longitude,
               |                latitude,
               |                user_agent
               |            from
               |            ods.ods_media_req where etl_date='${etlDate}' and time is not null and time!='' and app_id in (${appIdsStr})
               |        ) t
               |        group by app_id,id,adsolt_id
               |    ) t
               |) t1
               |left join
               |( -- 曝光日志
               |    select
               |        app_id,
               |        id,
               |        adsolt_id,
               |        imp_cnt, -- 曝光次数
               |        sum(imp_cnt) over(partition by id)/imp_cnt as imp_rate, -- 曝光次数占比
               |        creative_count, -- 创意次数
               |        max_count_creative, -- 曝光最多的创意
               |        avg_real_price,  -- 曝光平均价格
               |        max_real_price,  -- 曝光最高价格
               |        min_real_price   -- 曝光最低价格
               |    from
               |    (
               |        select
               |            app_id,
               |            id,
               |            adsolt_id,
               |            sum(imp_cnt) as imp_cnt, -- 曝光次数
               |            count(distinct creative_id) as creative_count, -- 创意种类次数
               |            max_count_col(creative_id) as max_count_creative, -- 曝光最多的创意
               |            avg(real_price) avg_real_price, -- 曝光平均价格
               |            max(real_price) max_real_price, -- 曝光最高价格
               |            min(real_price) min_real_price -- 曝光最低价格
               |        from
               |        (
               |            select
               |                case when os='1' then
               |                    case when not ( imei is null or imei ='' or imei = '000000000000000') then if(length(imei) = 32,imei,md5(imei))
               |                         when oaid is not null and oaid != '' and oaid !='00000000-0000-0000-0000-000000000000' then if(length(oaid) = 32,oaid,md5(oaid))
               |                         else null end
               |
               |                    when os ='2' then if(idfa is not null and idfa != '', if(length(idfa)=32,idfa,md5(idfa)) ,null)
               |                    else null end
               |                    as id,
               |                app_id, --媒体id
               |                adsolt_id, -- 广告位id,
               |                1 as imp_cnt, -- 曝光次数
               |                ----
               |                dsp_adsolt_id as creative_id, -- 创意id
               |                real_price -- 曝光价格
               |            from
               |                ods.ods_ssp_imp
               |            where etl_date='${etlDate}' and time is not null and time!='' and app_id in (${appIdsStr})
               |        ) t
               |        group by app_id,id,adsolt_id
               |    ) t
               |
               |) t2
               |on t1.app_id = t2.app_id and t1.id= t2.id and t1.adsolt_id = t2.adsolt_id
               |left join
               |( -- 点击日志
               |    select
               |        app_id,
               |        id,
               |        adsolt_id,
               |        clk_cnt,
               |        clk_cnt/sum(clk_cnt) over(partition by id) as clk_rate, -- 点击次数占比
               |        creative_count, -- 点击创意种类
               |        max_count_creative, -- 点击最多的创意
               |        time_elements[0] as first_seconds, -- 第一次在本时间段内的秒数
               |        time_elements[1] as last_seconds, -- 最后一次在本时间段内的秒数
               |        time_elements[2] as min_interval, -- 点击间隔最小时间
               |        time_elements[3] as max_interval, -- 点击间隔最大时间
               |        time_elements[4] as avg_interval -- 点击间隔平均时间
               |    from
               |    (
               |        select
               |            app_id,
               |            id,
               |            adsolt_id,
               |            sum(clk_cnt) as clk_cnt, -- 点击次数
               |            count(distinct creative_id) creative_count, -- 创意种类
               |            max_count_col(creative_id) max_count_creative, -- 点击最多的创意
               |            time_process(time) as time_elements
               |        from
               |        (
               |        select
               |            case when os='1' then
               |                case when not ( imei is null or imei ='' or imei = '000000000000000') then if(length(imei) = 32,imei,md5(imei))
               |                    when oaid is not null and oaid != '' and oaid !='00000000-0000-0000-0000-000000000000' then if(length(oaid) = 32,oaid,md5(oaid))
               |                    else null end
               |
               |                when os ='2' then if(idfa is not null and idfa != '', if(length(idfa)=32,idfa,md5(idfa)) ,null)
               |                else null end
               |                as id,
               |                app_id,
               |                adsolt_id,
               |                1 as clk_cnt, -- 点击次数
               |                ---
               |                dsp_adsolt_id as creative_id, -- 创意id
               |                time -- 时间戳/ 秒
               |        from
               |        ods.ods_ssp_clk where etl_date='${etlDate}' and time is not null and time!='' and app_id in (${appIdsStr})
               |        ) t
               |        group by app_id,id,adsolt_id
               |    ) t
               |) t3
               |on t1.app_id = t3.app_id and t1.id= t3.id and t1.adsolt_id = t3.adsolt_id
               |""".stripMargin).createOrReplaceTempView("behavior_day_with_media")


    }

    private def summaryDayNoMedia(spark: SparkSession, etlDate: String,appIdArray:Array[String]): Unit = {
        val appIdsStr = appIdArray.map(x => "'"+x+"'").mkString(",")
        spark.sql(
            s"""
               |select
               |    t1.id,
               |    t1.id_type,
               |    t1.os,
               |    t1.producer,
               |    t1.model,
               |    t1.osv,
               |    t1.brand,
               |    t1.adsolt_width,
               |    t1.adsolt_height,
               |    t1.carrier,
               |    t1.network,
               |    t1.city_id,
               |    t1.pkg_name,
               |    t1.adsolt_type,
               |    t1.real_adsolt_width,
               |    t1.real_adsolt_height,
               |    t1.req_count,
               |    t1.req_rate, -- 竞价请求次数占比
               |    t1.avg_real_price as req_avg_real_price, -- 竞价请求平均价格
               |    t1.max_real_price as req_max_real_price, -- 竞价请求最高价格
               |    t1.min_real_price as req_min_real_price,
               |    t1.req_app_count,  -- 请求媒体种类数量
               |    t2.imp_cnt , -- 曝光次数
               |    t2.imp_rate, -- 曝光次数占比
               |    t2.creative_count as imp_creative_count, -- 曝光创意种类次数
               |    t2.max_count_creative as imp_max_count_creative,  -- 曝光最多的创意
               |    t2.avg_real_price as imp_avg_real_price, -- 曝光平均价格
               |    t2.max_real_price as imp_max_real_price,
               |    t2.min_real_price as imp_min_real_price, -- 曝光最低价格
               |    t2.imp_app_count, -- 曝光媒体种类数量
               |    t3.clk_cnt,
               |    t3.clk_rate,
               |    t3.creative_count as clk_creative_count, -- 点击创意种类次数
               |    t3.max_count_creative as clk_max_count_creative,  -- 点击最多的创意
               |    t3.first_seconds as clk_first_seconds,
               |    t3.last_seconds as clk_last_seconds,
               |    t3.min_interval as clk_min_interval,
               |    t3.max_interval as clk_max_interval,
               |    t3.avg_interval as clk_avg_interval,
               |    t3.clk_app_count, -- 点击媒体种类数量
               |    t1.etl_date
               |from
               |( -- 媒体请求
               |    select
               |        id,
               |        id_type,
               |        os,
               |        producer,
               |        model,
               |        osv,
               |        brand,
               |        adsolt_width,
               |        adsolt_height,
               |        carrier,
               |        network,
               |        city_id,
               |        pkg_name,
               |        adsolt_type,
               |        real_adsolt_width,
               |        real_adsolt_height,
               |        etl_date,
               |        req_count,
               |        req_count/ sum(req_count) over(partition by id ) req_rate,  -- 竞价请求次数占比
               |        avg_real_price,
               |        max_real_price,
               |        min_real_price,
               |        req_app_count
               |    from
               |    (
               |        select
               |            id,
               |            max(id_type) as id_type, -- 设备id类型
               |            max(os) as os,
               |            max_count_col(producer) as producer,
               |            max(model) as model,
               |            max(osv) as osv,
               |            max(brand) as brand,
               |            max(adsolt_width) as adsolt_width,
               |            max(adsolt_height) as adsolt_height,
               |            max_count_col(carrier) as carrier,-- 运营商,取次数最多的一条
               |            max_count_col(network) as network,
               |            max_count_col(city_id) as city_id,
               |            max(pkg_name) as pkg_name,
               |            max(adsolt_type) as adsolt_type,
               |            max(real_adsolt_width) as real_adsolt_width,
               |            max(real_adsolt_height) as real_adsolt_height,
               |            max(etl_date) as etl_date,
               |            sum(req_count) as req_count, -- 竞价请求次数
               |            -- 竞价请求次数占比
               |            avg(floor_price) as avg_real_price,  -- 竞价请求平均价格
               |            max(floor_price) as max_real_price,-- 竞价请求最高价格
               |            min(floor_price) as min_real_price,-- 竞价请求最低价格
               |            count(distinct app_id) as req_app_count -- 请求媒体的数量
               |        from
               |        (
               |            select
               |                case when os='1' then
               |                    case when not ( imei is null or imei ='' or imei = '000000000000000') then if(length(imei) = 32,imei,md5(imei))
               |                         when oaid is not null and oaid != '' and oaid !='00000000-0000-0000-0000-000000000000' then if(length(oaid) = 32,oaid,md5(oaid))
               |                         else null end
               |
               |                    when os ='2' then if(idfa is not null and idfa != '', if(length(idfa)=32,idfa,md5(idfa)) ,null)
               |                    else null end
               |                    as id,
               |                app_id, --媒体id
               |                adsolt_id, -- 广告位id,
               |                if(os='1',if( imei is null or imei ='' or imei = '000000000000000',3,1),2) as id_type, -- 设备id类型, imei(1)/idfa(2)/oaid(3)
               |                os,
               |                producer, -- 制造商,出现次数最高的制造商
               |                model, -- 手机型号
               |                osv, -- 系统版本
               |                parse_brand(producer) as brand, -- udf过滤后的品牌
               |                adsolt_width, -- 屏幕宽
               |                adsolt_height, -- 屏幕高
               |                carrier, -- 运营商
               |                network, -- 网络连接类型
               |                city_id, -- 地域
               |                pkg_name,--, 包名..............................................
               |                adsolt_type, -- 广告位类型
               |                if(real_adsolt_shape like '%x%',split(real_adsolt_shape,'x')[0],null) as real_adsolt_width,-- 广告位宽
               |                if(real_adsolt_shape like '%x%',split(real_adsolt_shape,'x')[1],null) as real_adsolt_height,-- 广告位高
               |                etl_date, -- 日期
               |                1 as req_count, -- 竞价请求次数
               |                floor_price -- 竞价请求价格
               |            from
               |            ods.ods_media_req where etl_date='${etlDate}' and time is not null and time!='' and app_id in (${appIdsStr})
               |        ) t
               |        group by id
               |    ) t
               |) t1
               |left join
               |( -- 曝光日志
               |    select
               |        id,
               |        imp_cnt, -- 曝光次数
               |        sum(imp_cnt) over(partition by id)/imp_cnt as imp_rate, -- 曝光次数占比
               |        creative_count, -- 创意次数
               |        max_count_creative, -- 曝光最多的创意
               |        avg_real_price,  -- 曝光平均价格
               |        max_real_price,  -- 曝光最高价格
               |        min_real_price,   -- 曝光最低价格
               |        imp_app_count    -- 曝光媒体种类数量
               |    from
               |    (
               |        select
               |            id,
               |            sum(imp_cnt) as imp_cnt, -- 曝光次数
               |            count(distinct creative_id) as creative_count, -- 创意种类次数
               |            max_count_col(creative_id) as max_count_creative, -- 曝光最多的创意
               |            avg(real_price) avg_real_price, -- 曝光平均价格
               |            max(real_price) max_real_price, -- 曝光最高价格
               |            min(real_price) min_real_price, -- 曝光最低价格
               |            count(distinct app_id) as imp_app_count -- 曝光媒体种类数量
               |        from
               |        (
               |            select
               |                case when os='1' then
               |                    case when not ( imei is null or imei ='' or imei = '000000000000000') then if(length(imei) = 32,imei,md5(imei))
               |                         when oaid is not null and oaid != '' and oaid !='00000000-0000-0000-0000-000000000000' then if(length(oaid) = 32,oaid,md5(oaid))
               |                         else null end
               |
               |                    when os ='2' then if(idfa is not null and idfa != '', if(length(idfa)=32,idfa,md5(idfa)) ,null)
               |                    else null end
               |                    as id,
               |                app_id, --媒体id
               |                adsolt_id, -- 广告位id,
               |                1 as imp_cnt, -- 曝光次数
               |                ----
               |                dsp_adsolt_id as creative_id, -- 创意id
               |                real_price -- 曝光价格
               |            from
               |                ods.ods_ssp_imp
               |            where etl_date='${etlDate}' and time is not null and time!='' and app_id in (${appIdsStr})
               |        ) t
               |        group by id
               |    ) t
               |
               |) t2
               |on  t1.id= t2.id
               |left join
               |( -- 点击日志
               |    select
               |        id,
               |        clk_cnt,
               |        clk_cnt/sum(clk_cnt) over(partition by id) as clk_rate, -- 点击次数占比
               |        creative_count, -- 点击创意种类
               |        max_count_creative, -- 点击最多的创意
               |        time_elements[0] as first_seconds, -- 第一次在本时间段内的秒数
               |        time_elements[1] as last_seconds, -- 最后一次在本时间段内的秒数
               |        time_elements[2] as min_interval, -- 点击间隔最小时间
               |        time_elements[3] as max_interval, -- 点击间隔最大时间
               |        time_elements[4] as avg_interval, -- 点击间隔平均时间
               |        clk_app_count
               |    from
               |    (
               |        select
               |            id,
               |            sum(clk_cnt) as clk_cnt, -- 点击次数
               |            count(distinct creative_id) creative_count, -- 创意种类
               |            max_count_col(creative_id) max_count_creative, -- 点击最多的创意
               |            time_process(time) as time_elements,
               |            count(distinct app_id) as clk_app_count -- 点击媒体种类数量
               |        from
               |        (
               |        select
               |            case when os='1' then
               |                case when not ( imei is null or imei ='' or imei = '000000000000000') then if(length(imei) = 32,imei,md5(imei))
               |                    when oaid is not null and oaid != '' and oaid !='00000000-0000-0000-0000-000000000000' then if(length(oaid) = 32,oaid,md5(oaid))
               |                    else null end
               |
               |                when os ='2' then if(idfa is not null and idfa != '', if(length(idfa)=32,idfa,md5(idfa)) ,null)
               |                else null end
               |                as id,
               |                app_id,
               |                adsolt_id,
               |                1 as clk_cnt, -- 点击次数
               |                ---
               |                dsp_adsolt_id as creative_id, -- 创意id
               |                time -- 时间戳/ 秒
               |        from
               |        ods.ods_ssp_clk where etl_date='${etlDate}' and time is not null and time!='' and app_id in (${appIdsStr})
               |        ) t
               |        group by id
               |    ) t where time_elements[0] != 0.0
               |) t3
               |on  t1.id= t3.id
               |
               |""".stripMargin).createOrReplaceTempView("behavior_day_no_media")

    }


    /**
     *
     * 3天的统计
     *
     * @param spark
     * @param etlDate
     * @param calculateDays
     */
    def summaryCalculateDay(spark:SparkSession,etlDate:String,calculateDays:Int,appIdArray:Array[String]): Unit ={
        val appIdsStr = appIdArray.map(x => "'"+x+"'").mkString(",")

        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val date = sdf.parse(etlDate)
        val headDate = sdf.format(new Date(date.getTime - (calculateDays -1) * 24 * 3600 * 1000)) // 包括

        spark.sql(
            s"""
               |select
               |    t1.app_id,
               |    t1.id,
               |    t1.adsolt_id,
               |    t1.req_count,
               |    t1.req_rate, -- 竞价请求次数占比
               |    t1.avg_real_price as req_avg_real_price, -- 竞价请求平均价格
               |    t1.max_real_price as req_max_real_price, -- 竞价请求最高价格
               |    t1.min_real_price as req_min_real_price,
               |    t2.imp_cnt , -- 曝光次数
               |    t2.imp_rate, -- 曝光次数占比
               |    t2.creative_count as imp_creative_count, -- 曝光创意种类次数
               |    t2.max_count_creative as imp_max_count_creative,  -- 曝光最多的创意
               |    t2.avg_real_price as imp_avg_real_price, -- 曝光平均价格
               |    t2.max_real_price as imp_max_real_price,
               |    t2.min_real_price as imp_min_real_price, -- 曝光最低价格
               |    t3.clk_cnt,
               |    t3.clk_rate,
               |    t3.creative_count as clk_creative_count, -- 点击创意种类次数
               |    t3.max_count_creative as clk_max_count_creative  -- 点击最多的创意
               |from
               |( -- 媒体请求
               |    select
               |        app_id,
               |        id,
               |        adsolt_id,
               |        id_type,
               |        os,
               |        producer,
               |        model,
               |        osv,
               |        brand,
               |        adsolt_width,
               |        adsolt_height,
               |        carrier,
               |        network,
               |        city_id,
               |        pkg_name,
               |        adsolt_type,
               |        real_adsolt_width,
               |        real_adsolt_height,
               |        etl_date,
               |        req_count,
               |        req_count/ sum(req_count) over(partition by id ) req_rate,  -- 竞价请求次数占比
               |        avg_real_price,
               |        max_real_price,
               |        min_real_price,
               |        hour,
               |        longitude,
               |        latitude,
               |        ua
               |    from
               |    (
               |        select
               |            app_id,
               |            id,
               |            adsolt_id,
               |            max(id_type) as id_type, -- 设备id类型
               |            max(os) as os,
               |            max_count_col(producer) as producer,
               |            max(model) as model,
               |            max(osv) as osv,
               |            max(brand) as brand,
               |            max(adsolt_width) as adsolt_width,
               |            max(adsolt_height) as adsolt_height,
               |            max_count_col(carrier) as carrier,-- 运营商,取次数最多的一条
               |            max_count_col(network) as network,
               |            max_count_col(city_id) as city_id,
               |            max(pkg_name) as pkg_name,
               |            max(adsolt_type) as adsolt_type,
               |            max(real_adsolt_width) as real_adsolt_width,
               |            max(real_adsolt_height) as real_adsolt_height,
               |            max(etl_date) as etl_date,
               |            sum(req_count) as req_count, -- 竞价请求次数
               |            -- 竞价请求次数占比
               |            avg(floor_price) as avg_real_price,  -- 竞价请求平均价格
               |            max(floor_price) as max_real_price,-- 竞价请求最高价格
               |            min(floor_price) as min_real_price,-- 竞价请求最低价格
               |            max(etl_hour) as hour,
               |            max(longitude) as longitude,
               |            max(latitude) as latitude,
               |            max(user_agent) as ua
               |        from
               |        (
               |            select
               |                case when os='1' then
               |                    case when not ( imei is null or imei ='' or imei = '000000000000000') then if(length(imei) = 32,imei,md5(imei))
               |                         when oaid is not null and oaid != '' and oaid !='00000000-0000-0000-0000-000000000000' then if(length(oaid) = 32,oaid,md5(oaid))
               |                         else null end
               |
               |                    when os ='2' then if(idfa is not null and idfa != '', if(length(idfa)=32,idfa,md5(idfa)) ,null)
               |                    else null end
               |                    as id,
               |                app_id, --媒体id
               |                adsolt_id, -- 广告位id,
               |                if(os='1',if( imei is null or imei ='' or imei = '000000000000000',3,1),2) as id_type, -- 设备id类型, imei(1)/idfa(2)/oaid(3)
               |                os,
               |                producer, -- 制造商,出现次数最高的制造商
               |                model, -- 手机型号
               |                osv, -- 系统版本
               |                parse_brand(producer) as brand, -- udf过滤后的品牌
               |                adsolt_width, -- 屏幕宽
               |                adsolt_height, -- 屏幕高
               |                carrier, -- 运营商
               |                network, -- 网络连接类型
               |                city_id, -- 地域
               |                pkg_name,--, 包名..............................................
               |                adsolt_type, -- 广告位类型
               |                if(real_adsolt_shape like '%x%',split(real_adsolt_shape,'x')[0],null) as real_adsolt_width,-- 广告位宽
               |                if(real_adsolt_shape like '%x%',split(real_adsolt_shape,'x')[1],null) as real_adsolt_height,-- 广告位高
               |                etl_date, -- 日期
               |                1 as req_count, -- 竞价请求次数
               |                floor_price, -- 竞价请求价格
               |                etl_hour,
               |                longitude,
               |                latitude,
               |                user_agent
               |            from
               |            ods.ods_media_req where etl_date >='${headDate}' and etl_date <='${etlDate}' and  time is not null and time!='' and app_id in (${appIdsStr})
               |        ) t
               |        group by app_id,id,adsolt_id
               |    ) t
               |) t1
               |left join
               |( -- 曝光日志
               |    select
               |        app_id,
               |        id,
               |        adsolt_id,
               |        imp_cnt, -- 曝光次数
               |        sum(imp_cnt) over(partition by id)/imp_cnt as imp_rate, -- 曝光次数占比
               |        creative_count, -- 创意次数
               |        max_count_creative, -- 曝光最多的创意
               |        avg_real_price,  -- 曝光平均价格
               |        max_real_price,  -- 曝光最高价格
               |        min_real_price   -- 曝光最低价格
               |    from
               |    (
               |        select
               |            app_id,
               |            id,
               |            adsolt_id,
               |            sum(imp_cnt) as imp_cnt, -- 曝光次数
               |            count(distinct creative_id) as creative_count, -- 创意种类次数
               |            max_count_col(creative_id) as max_count_creative, -- 曝光最多的创意
               |            avg(real_price) avg_real_price, -- 曝光平均价格
               |            max(real_price) max_real_price, -- 曝光最高价格
               |            min(real_price) min_real_price -- 曝光最低价格
               |        from
               |        (
               |            select
               |                case when os='1' then
               |                    case when not ( imei is null or imei ='' or imei = '000000000000000') then if(length(imei) = 32,imei,md5(imei))
               |                         when oaid is not null and oaid != '' and oaid !='00000000-0000-0000-0000-000000000000' then if(length(oaid) = 32,oaid,md5(oaid))
               |                         else null end
               |
               |                    when os ='2' then if(idfa is not null and idfa != '', if(length(idfa)=32,idfa,md5(idfa)) ,null)
               |                    else null end
               |                    as id,
               |                app_id, --媒体id
               |                adsolt_id, -- 广告位id,
               |                1 as imp_cnt, -- 曝光次数
               |                ----
               |                dsp_adsolt_id as creative_id, -- 创意id
               |                real_price -- 曝光价格
               |            from
               |                ods.ods_ssp_imp
               |            where etl_date >='${headDate}' and etl_date <='${etlDate}' and time is not null and time!='' and app_id in (${appIdsStr})
               |        ) t
               |        group by app_id,id,adsolt_id
               |    ) t
               |
               |) t2
               |on t1.app_id = t2.app_id and t1.id= t2.id and t1.adsolt_id = t2.adsolt_id
               |left join
               |( -- 点击日志
               |    select
               |        app_id,
               |        id,
               |        adsolt_id,
               |        clk_cnt,
               |        clk_cnt/sum(clk_cnt) over(partition by id) as clk_rate, -- 点击次数占比
               |        creative_count, -- 点击创意种类
               |        max_count_creative -- 点击最多的创意
               |    from
               |    (
               |        select
               |            app_id,
               |            id,
               |            adsolt_id,
               |            sum(clk_cnt) as clk_cnt, -- 点击次数
               |            count(distinct creative_id) creative_count, -- 创意种类
               |            max_count_col(creative_id) max_count_creative -- 点击最多的创意
               |        from
               |        (
               |        select
               |            case when os='1' then
               |                case when not ( imei is null or imei ='' or imei = '000000000000000') then if(length(imei) = 32,imei,md5(imei))
               |                    when oaid is not null and oaid != '' and oaid !='00000000-0000-0000-0000-000000000000' then if(length(oaid) = 32,oaid,md5(oaid))
               |                    else null end
               |
               |                when os ='2' then if(idfa is not null and idfa != '', if(length(idfa)=32,idfa,md5(idfa)) ,null)
               |                else null end
               |                as id,
               |                app_id,
               |                adsolt_id,
               |                1 as clk_cnt, -- 点击次数
               |                ---
               |                dsp_adsolt_id as creative_id, -- 创意id
               |                time -- 时间戳/ 秒
               |        from
               |        ods.ods_ssp_clk where etl_date >='${headDate}' and etl_date <='${etlDate}' and time is not null and time!='' and app_id in (${appIdsStr})
               |        ) t
               |        group by app_id,id,adsolt_id
               |    ) t
               |) t3
               |on t1.app_id = t3.app_id and t1.id= t3.id and t1.adsolt_id = t3.adsolt_id
               |
               |""".stripMargin).createOrReplaceTempView("behavior_with_media_D"+calculateDays)


    }

    //新增ocpx关联
    /*private*/ def  mediaLinkOcpx(spark: SparkSession, etlDate: String, appIdArray:Array[String]): Unit ={
        val appIdsStr: String = appIdArray.map(x => "'"+x+"'").mkString(",")
        spark.sql(
            s"""
               |select
               |     t1.app_id,
               |     t1.id,
               |     t1.adsolt_id,
               |     t2.plan_id,
               |     t2.create_id,
               |     t2.admaster_id,
               |     t2.ocpx_tag,
               |     t2.if_new_enter,
               |     t2.if_live_awaken,
               |     t2.if_pay,
               |     t2.if_mau
               |from
               |(
               |    select
               |         case when os='1' then
               |                   case when not ( imei is null or imei ='' or imei = '000000000000000') then if(length(imei) = 32,imei,md5(imei))
               |                        when oaid is not null and oaid != '' and oaid !='00000000-0000-0000-0000-000000000000' then if(length(oaid) = 32,oaid,md5(oaid))
               |                   else null end
               |              when os ='2' then if(idfa is not null and idfa != '', if(length(idfa)=32,idfa,md5(idfa)) ,null)
               |              else null end
               |              as id,
               |         app_id, --媒体id
               |         adsolt_id, -- 广告位id
               |         request_id   --请求id
               |    from
               |    ods.ods_media_req where etl_date='${etlDate}' and time is not null and time!='' and app_id in (${appIdsStr})
               |) t1
               |join
               |(
               |   select
               |       media_id,
               |       adslot_id,
               |       case when os='1' then
               |                 case when not ( imei is null or imei ='' or imei = '000000000000000') then if(length(imei) = 32,imei,md5(imei))
               |                      when oaid is not null and oaid != '' and oaid !='00000000-0000-0000-0000-000000000000' then if(length(oaid) = 32,oaid,md5(oaid))
               |                      else null end
               |            when os ='2' then if(idfa is not null and idfa != '', if(length(idfa)=32,idfa,md5(idfa)) ,null)
               |            else null end
               |            as id,
               |        plan_id,
               |        creative_id as create_id,
               |        admaster_id,
               |       ocpx_tag,
               |       case when ocpx_tag = 'alp_1' then 1 else 0 end as if_new_enter,
               |       case when ocpx_tag = 'alp_2' then 1 else 0 end as if_live_awaken,
               |       case when ocpx_tag = 'alp_3' then 1 else 0 end as if_pay,
               |       case when ocpx_tag = 'alp_4' then 1 else 0 end as if_mau
               |    from
               |       ods.req_bid_imp_clk_55  where etl_date='${etlDate}' and time is not null and time!=''
               |) t2
               |on  t1.app_id=t2.media_id and t1.id=t2.id and t1.adsolt_id=t2.adslot_id
               |""".stripMargin).createOrReplaceTempView("behavior_media_with_ocpx")


    }
}
