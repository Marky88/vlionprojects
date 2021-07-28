package com.vlion.statistics

import com.vlion.udfs.{MaxCountColUDAF, ParseBrandUDF, TimeUDAF}
import org.apache.spark.sql.SparkSession


/**
 * @description:
 * @author: malichun
 * @time: 2021/7/23/0023 16:17
 *
 */
object Statistics {


    def summaryDay(spark: SparkSession, etlDate: String): Unit = {
        spark.udf.register("max_count_col", new MaxCountColUDAF)
        spark.udf.register("time_process", new TimeUDAF(etlDate))
        spark.udf.register("parse_brand", new ParseBrandUDF().parseBrand _ )


        spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        spark.sql("set hive.exec.dynamic.partition =true")
        // 分媒体,广告位id
        spark.sql(
            s"""
               |insert overwrite table behavior.behavior_summary_day partition(etl_date)
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
               |    t1.etl_date
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
               |        min_real_price
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
               |            min(floor_price) as min_real_price-- 竞价请求最低价格
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
               |            ods.ods_media_req where etl_date='${etlDate}'
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
               |            where etl_date='${etlDate}'
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
               |        ods.ods_ssp_clk where etl_date='${etlDate}'
               |        ) t
               |        group by app_id,id,adsolt_id
               |    ) t
               |) t3
               |on t1.app_id = t3.app_id and t1.id= t3.id and t1.adsolt_id = t3.adsolt_id
               |
               |""".stripMargin)


    }

    /**
     * 通过每天的报告,生成前3天,7天,14天的文件,覆盖更新
     * @param spark
     * @param etlDate
     * @param calculateDays
     */
    def summaryCalculateDay(spark:SparkSession,etlDate:String,calculateDays:Int): Unit ={

    }

    def summaryHour(spark:SparkSession, etlDate: String,etlHour:String) ={
        // 小时的统计
        spark.sql(
            s"""
               |select
               |        t1.app_id,
               |        t1.id,
               |        t1.adsolt_id,
               |        t1.id_type,
               |        t1.os,
               |        t1.producer,
               |        t1.model,
               |        t1.osv,
               |        t1.brand,
               |        t1.adsolt_width,
               |        t1.adsolt_height,
               |        t1.carrier,
               |        t1.network,
               |        t1.city_id,
               |        t1.pkg_name,
               |        t1.adsolt_type,
               |        t1.real_adsolt_width,
               |        t1.real_adsolt_height,
               |        t1.etl_hour,
               |        t1.etl_date,
               |        t1.req_count,
               |        t1.req_rate, -- 竞价请求次数占比
               |        t1.avg_real_price as req_avg_real_price, -- 竞价请求平均价格
               |        t1.max_real_price as req_max_real_price, -- 竞价请求最高价格
               |        t1.min_real_price as req_min_real_price,
               |        t2.imp_cnt , -- 曝光次数
               |        t2.imp_rate, -- 曝光次数占比
               |        t2.creative_count as imp_creative_count, -- 曝光创意种类次数
               |        t2.max_count_creative as imp_max_count_creative,  -- 曝光最多的创意
               |        t2.avg_real_price as imp_avg_real_price, -- 曝光平均价格
               |        t2.max_real_price as imp_max_real_price,
               |        t2.min_real_price as imp_min_real_price, -- 曝光最低价格
               |        t3.clk_cnt,
               |        t3.clk_rate,
               |        t3.creative_count as clk_creative_count, -- 点击创意种类次数
               |        t3.max_count_creative as clk_max_count_creative,  -- 点击最多的创意
               |        t3.first_seconds as clk_first_seconds,
               |        t3.last_seconds as clk_last_seconds,
               |        t3.min_interval as clk_min_interval,
               |        t3.max_interval as clk_max_interval,
               |        t3.avg_interval as clk_avg_interval
               |    from
               |    ( -- 媒体请求
               |        select
               |            app_id,
               |            id,
               |            adsolt_id,
               |            id_type,
               |            os,
               |            producer,
               |            model,
               |            osv,
               |            brand,
               |            adsolt_width,
               |            adsolt_height,
               |            carrier,
               |            network,
               |            city_id,
               |            pkg_name,
               |            adsolt_type,
               |            real_adsolt_width,
               |            real_adsolt_height,
               |            etl_hour,
               |            etl_date,
               |            req_count,
               |            req_count/ sum(req_count) over(partition by id ) req_rate,  -- 竞价请求次数占比
               |            avg_real_price,
               |            max_real_price,
               |            min_real_price
               |        from
               |        (
               |            select
               |                app_id,
               |                id,
               |                adsolt_id,
               |                max(id_type) as id_type, -- 设备id类型
               |                max(os) as os,
               |                max_count_col(producer) as producer,
               |                max(model) as model,
               |                max(osv) as osv,
               |                max(brand) as brand,
               |                max(adsolt_width) as adsolt_width,
               |                max(adsolt_height) as adsolt_height,
               |                max_count_col(carrier) as carrier,-- 运营商,取次数最多的一条
               |                max_count_col(network) as network,
               |                max_count_col(city_id) as city_id,
               |                max(pkg_name) as pkg_name,
               |                max(adsolt_type) as adsolt_type,
               |                max(real_adsolt_width) as real_adsolt_width,
               |                max(real_adsolt_height) as real_adsolt_height,
               |                max(etl_hour) as etl_hour,
               |                max(etl_date) as etl_date,
               |                sum(req_count) as req_count, -- 竞价请求次数
               |                -- 竞价请求次数占比
               |                avg(floor_price) as avg_real_price,  -- 竞价请求平均价格
               |                max(floor_price) as max_real_price,-- 竞价请求最高价格
               |                min(floor_price) as min_real_price-- 竞价请求最低价格
               |            from
               |            (
               |                select
               |                    case when os='1' then
               |                        case when not ( imei is null or imei ='' or imei = '000000000000000') then if(length(imei) = 32,imei,md5(imei))
               |                             when oaid is not null and oaid != '' and oaid !='00000000-0000-0000-0000-000000000000' then if(length(oaid) = 32,oaid,md5(oaid))
               |                             else null end
               |
               |                        when os ='2' then if(idfa is not null and idfa != '', if(length(idfa)=32,idfa,md5(idfa)) ,null)
               |                        else null end
               |                        as id,
               |                    app_id, --媒体id
               |                    adsolt_id, -- 广告位id,
               |                    if(os='1',if( imei is null or imei ='' or imei = '000000000000000',3,1),2) as id_type, -- 设备id类型, imei(1)/idfa(2)/oaid(3)
               |                    os,
               |                    producer, -- 制造商,出现次数最高的制造商
               |                    model, -- 手机型号
               |                    osv, -- 系统版本
               |                    parse_brand(producer) as brand, -- udf过滤后的品牌
               |                    adsolt_width, -- 屏幕宽
               |                    adsolt_height, -- 屏幕高
               |                    carrier, -- 运营商
               |                    network, -- 网络连接类型
               |                    city_id, -- 地域
               |                    pkg_name,--, 包名..............................................
               |                    adsolt_type, -- 广告位类型
               |                    if(real_adsolt_shape like '%x%',split(real_adsolt_shape,'x')[0],null) as real_adsolt_width,-- 广告位宽
               |                    if(real_adsolt_shape like '%x%',split(real_adsolt_shape,'x')[1],null) as real_adsolt_height,-- 广告位高
               |                    etl_hour, -- 小时
               |                    etl_date, -- 日期
               |                    1 as req_count, -- 竞价请求次数
               |                    floor_price -- 竞价请求价格
               |                from
               |                ods.ods_media_req where etl_date='${etlDate}' and etl_hour='${etlHour}'
               |            ) t
               |            group by app_id,id,adsolt_id
               |        ) t
               |    ) t1
               |    left join
               |    ( -- 曝光日志
               |        select
               |            app_id,
               |            id,
               |            adsolt_id,
               |            imp_cnt, -- 曝光次数
               |            sum(imp_cnt) over(partition by id)/imp_cnt as imp_rate, -- 曝光次数占比
               |            creative_count, -- 创意次数
               |            max_count_creative, -- 曝光最多的创意
               |            avg_real_price,  -- 曝光平均价格
               |            max_real_price,  -- 曝光最高价格
               |            min_real_price   -- 曝光最低价格
               |        from
               |        (
               |            select
               |                app_id,
               |                id,
               |                adsolt_id,
               |                sum(imp_cnt) as imp_cnt, -- 曝光次数
               |                count(distinct creative_id) as creative_count, -- 创意种类次数
               |                max_count_col(creative_id) as max_count_creative, -- 曝光最多的创意
               |                avg(real_price) avg_real_price, -- 曝光平均价格
               |                max(real_price) max_real_price, -- 曝光最高价格
               |                min(real_price) min_real_price -- 曝光最低价格
               |            from
               |            (
               |                select
               |                    case when os='1' then
               |                        case when not ( imei is null or imei ='' or imei = '000000000000000') then if(length(imei) = 32,imei,md5(imei))
               |                             when oaid is not null and oaid != '' and oaid !='00000000-0000-0000-0000-000000000000' then if(length(oaid) = 32,oaid,md5(oaid))
               |                             else null end
               |
               |                        when os ='2' then if(idfa is not null and idfa != '', if(length(idfa)=32,idfa,md5(idfa)) ,null)
               |                        else null end
               |                        as id,
               |                    app_id, --媒体id
               |                    adsolt_id, -- 广告位id,
               |                    1 as imp_cnt, -- 曝光次数
               |                    ----
               |                    dsp_adsolt_id as creative_id, -- 创意id
               |                    real_price -- 曝光价格
               |                from
               |                    ods.ods_ssp_imp
               |                where etl_date='${etlDate}' and etl_hour='${etlHour}'
               |            ) t
               |            group by app_id,id,adsolt_id
               |        ) t
               |
               |    ) t2
               |    on t1.app_id = t2.app_id and t1.id= t2.id and t1.adsolt_id = t2.adsolt_id
               |    left join
               |    ( -- 点击日志
               |        select
               |            app_id,
               |            id,
               |            adsolt_id,
               |            clk_cnt,
               |            clk_cnt/sum(clk_cnt) over(partition by id) as clk_rate, -- 点击次数占比
               |            creative_count, -- 点击创意种类
               |            max_count_creative, -- 点击最多的创意
               |            time_elements[0] as first_seconds, -- 第一次在本时间段内的秒数
               |            time_elements[1] as last_seconds, -- 最后一次在本时间段内的秒数
               |            time_elements[2] as min_interval, -- 点击间隔最小时间
               |            time_elements[3] as max_interval, -- 点击间隔最大时间
               |            time_elements[4] as avg_interval -- 点击间隔平均时间
               |        from
               |        (
               |            select
               |                app_id,
               |                id,
               |                adsolt_id,
               |                sum(clk_cnt) as clk_cnt, -- 点击次数
               |                count(distinct creative_id) creative_count, -- 创意种类
               |                max_count_col(creative_id) max_count_creative, -- 点击最多的创意
               |                time_process(time) as time_elements
               |            from
               |            (
               |            select
               |                case when os='1' then
               |                    case when not ( imei is null or imei ='' or imei = '000000000000000') then if(length(imei) = 32,imei,md5(imei))
               |                        when oaid is not null and oaid != '' and oaid !='00000000-0000-0000-0000-000000000000' then if(length(oaid) = 32,oaid,md5(oaid))
               |                        else null end
               |
               |                    when os ='2' then if(idfa is not null and idfa != '', if(length(idfa)=32,idfa,md5(idfa)) ,null)
               |                    else null end
               |                    as id,
               |                    app_id,
               |                    adsolt_id,
               |                    1 as clk_cnt, -- 点击次数
               |                    ---
               |                    dsp_adsolt_id as creative_id, -- 创意id
               |                    time -- 时间戳/ 秒
               |            from
               |            ods.ods_ssp_clk where etl_date='${etlDate}' and etl_hour='${etlHour}'
               |            ) t
               |            group by app_id,id,adsolt_id
               |        ) t
               |    ) t3
               |    on t1.app_id = t3.app_id and t1.id= t3.id and t1.adsolt_id = t3.adsolt_id
               |""".stripMargin)

    }
}
