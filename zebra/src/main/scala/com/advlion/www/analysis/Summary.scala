package com.advlion.www.analysis

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.advlion.www.jdbc.MySQL
import com.advlion.www.parse.SAAccessLogParse
import org.apache.spark.sql.SparkSession

object Summary {
    val url: String = MySQL.url
    val user: String = MySQL.user
    val password: String = MySQL.password
    val driver: String = MySQL.driver
    //  val saAccessTable = "sa_access" //替换表,需要改表结构
    val saAccessTable = "sa_access_new"
    val saAccessWrongIpTable = "sa_access_wrong_ip"
    val saAccessCountTable = "sa_access_count" //第三个任务
    val statTable = "stat_day"
    val AccessTable = "access"

    def readSaAccess(spark: SparkSession, args: Array[String]): Unit = {
        val etlDate = args(0).toString
        println("执行时间：" + etlDate) //    val saAccessDF = spark.sql(
        //      s"""
        //         |select date_format(hour,'yyyy-MM-dd') as time,
        //         |		a.hour,
        //         |		a.domain,
        //         |		a.chid,
        //         |		a.template_id,
        //         |		a.page,
        //         |		a.adflag,
        //         |		a.adslocation_id,
        //         |		a.user_ip as ip,
        //         |		a.package_name,
        //         |		sum(case when a.call='call' then 1 else 0 end) impression,
        //         |		sum(case when a.call='resp' then 1 else 0 end) resp,
        //         |		sum(case when a.call='app' then 1 else 0 end) apps,
        //         |		sum(case when a.call='clk' then 1 else 0 end) clicks
        //         |from
        //         |	(
        //         |		select date_format(split(regexp_replace(time,'T',' '),'-07:00')[0],'yyyy-MM-dd HH:00:00') as hour,
        //         |				split(split(request,'&')[0],'=')[1] as call,
        //         |				split(split(split(request,'&')[1],'=')[1],' ')[0] as domain,
        //         |				split(split(request,'&')[2],'=')[1] as chid,
        //         |				split(split(request,'&')[3],'=')[1] as template_id,
        //         |				split(split(request,'&')[4],'=')[1] as page,
        //         |				split(split(request,'&')[5],'=')[1] as adflag,
        //         |				split(split(split(request,'&')[6],'=')[1],' ')[0] as adslocation_id,
        //         |				user_ip,
        //         |				package_name
        //         |		from ods.ods_sa_access
        //         |		where etl_date = '$etlDate'
        //         |    and referer <> "http://horoscope.1g18.com/detail.ht%D9%85%D8%AC%D8%A7%D8%B2%DB%8C%20%20%D8%B9%D9%84%D9%88%D9%85%20%D9%88%20%D8%AA%D8%AD%D9%82%DB%8C%D9%82%D8%A7%D8%AAml?q=E4B756130E43B737DF3A8F8CB30D43023AE24C737D157953DDCDCFF92E1897E5158FDD1975693C46"
        //         |	) a
        //         |group by date_format(hour,'yyyy-MM-dd'),a.hour,a.domain,a.chid,a.template_id,a.page,a.adflag,a.adslocation_id,a.user_ip,a.package_name
        //         |""".stripMargin)
        //////////////sa_access日志解析
        //        val saAccessDF = spark.sql(
        //          s"""
        //             |select date_format(hour,'yyyy-MM-dd') as time,
        //             |		a.hour,
        //             |		a.domain,
        //             |		a.chid,
        //             |		a.template_id,
        //             |		a.page,
        //             |		a.adflag,
        //             |		a.adslocation_id,
        //             |		a.user_ip as ip,
        //             |		a.package_name,
        //             |		sum(case when a.call='call' then 1 else 0 end) impression,
        //             |		sum(case when a.call='resp' then 1 else 0 end) resp,
        //             |		sum(case when a.call='app' then 1 else 0 end) apps,
        //             |		sum(case when a.call='clk' then 1 else 0 end) clicks
        //             |from
        //             |	(
        //             |		select date_format(split(regexp_replace(time,'T',' '),'-07:00')[0],'yyyy-MM-dd HH:00:00') as hour,
        //             |        split(split(split(request,' ')[1], 'act=')[1],'&')[0] as call,
        //             |        split(split(split(request,' ')[1], 'domain=')[1],'&')[0] as domain,
        //             |        split(split(split(request,' ')[1], 'chid=')[1],'&')[0] as chid,
        //             |        split(split(split(request,' ')[1], 'template_id=')[1],'&')[0] as template_id,
        //             |        split(split(split(request,' ')[1], 'page=')[1],'&')[0] as page,
        //             |        split(split(split(request,' ')[1], 'ad_flag=')[1],'&')[0] as adflag,
        //             |        split(split(request,' ')[1], '&_id=')[1] as adslocation_id,
        //             |				user_ip,
        //             |				package_name
        //             |		from ods.ods_sa_access
        //             |		where etl_date = '$etlDate'
        //             |    and length(split(split(split(request,' ')[1], 'page=')[1],'&')[0]) <=100
        //             |    and length(split(split(split(request,' ')[1], 'domain=')[1],'&')[0]) <=200
        //             |	) a
        //             |group by date_format(hour,'yyyy-MM-dd'),a.hour,a.domain,a.chid,a.template_id,a.page,a.adflag,a.adslocation_id,a.user_ip,a.package_name
        //             |""".stripMargin)
        /**
         * 任务一：按小时统计出每个广告位的曝光、未返回、点击数；
         *
         * 表结构示例：
         *
         * 时间点、广告位ID、域名、渠道ID、模板ID、访问页面、广告位标识、曝光数、未返回数、点击数
         */
        spark.udf.register("to_map", SAAccessLogParse.parseLog _)
        val saAccessDF = spark.sql(
            s"""|
                |select date_format(hour,'yyyy-MM-dd') as time,
                |        a.hour,
                |        a.adslocation_id,
                |        a.domain,
                |        a.chid,
                |        a.template_id,
                |        a.page,
                |        a.adflag,
                |        sum(case when a.call='call' then 1 else 0 end) impression,
                |        sum(case when a.call='resp' then 1 else 0 end) resp,
                |        sum(case when a.call='clk' then 1 else 0 end) clicks
                |from
                |        (
                |        select
                |        date_format(split(regexp_replace(time,'T',' '),'-07:00')[0],'yyyy-MM-dd HH:00:00') as hour,
                |        str_map['act'] as call,
                |        str_map['domain'] as domain,
                |        str_map['chid'] as chid,
                |        str_map['template_id'] as template_id,
                |        str_map['page'] as page,
                |        str_map['ad_flag'] as adflag,
                |        str_map['_id'] as adslocation_id
                |        from
                |     (
                |        select
                |        time     ,
                |        to_map(request) as  str_map    ,
                |        status        ,
                |        local_ip      ,
                |        user_ip       ,
                |        referer       ,
                |        user_agent    ,
                |        package_name  ,
                |        etl_date
                |        from
                |            ods.ods_sa_access
                |        where etl_date = '$etlDate'
                |
                |
                |    ) t
                |    ) a
                |    where length(a.page)<=100 and length(domain) <=200
                |group by date_format(hour,'yyyy-MM-dd'),a.hour,a.domain,a.chid,a.template_id,a.page,a.adflag,a.adslocation_id
                |""".stripMargin)


        saAccessDF.createOrReplaceTempView("saAccess")
        val saAccessDF2 =spark.sql(
            s"""
               |select
               |    a.time,
               |    a.hour,
               |    coalesce(b.tags_map_id,a.adslocation_id) as adslocation_id,
               |    coalesce(c.id,a.domain) as domain,
               |    coalesce(d.id,a.chid) as chid,
               |    a.template_id,
               |    a.page,
               |    a.adflag,
               |    b.user_id,
               |    b.station_id,
               |    b.link_id,
               |    a.impression,
               |    a.resp,
               |    a.clicks
               |
               |from
               |saAccess a
               |left join
               |tagsMapLink b
               |on a.adslocation_id= b.tags_md5
               |left join
               |url c
               |on a.domain = c.url
               |left join
               |channelIdMap d
               |on a.chid= d.channel_id
               |where length(a.adflag) <= 100
               |
               |""".stripMargin)
        println("sa_access 任务一:聚和后的条数:"+saAccessDF2.count())
        saAccessDF2.show()

        saAccessDF2.write.mode("append")
            .format("jdbc")
            .option("url", url)
            .option("driver", driver)
            .option("user", user)
            .option("password", password)
            .option("dbtable", saAccessTable)
            .save()
        /*
       *任务二：IP统计

        一天之内对同一广告位请求（曝光数）超过20次的IP；
        一天之内对同一广告位请求超过5次且广告未返回次数和请求次数的比值低于50%的IP；
        一小时之内对同一广告位点击超过3次的IP。
       */ val tempSaAccessDF = spark.sql(
            s"""
               |select
               |    date_format(split(regexp_replace(time,'T',' '),'-07:00')[0],'yyyy-MM-dd HH:00:00') as hour,
               |    str_map['act'] as call,
               |    str_map['_id'] as adslocation_id,
               |    user_ip
               |from
               |(
               |select
               |time     ,
               |to_map(request) as  str_map    ,
               |user_ip
               |from
               |ods.ods_sa_access
               |where etl_date='$etlDate'
               |) t
               |""".stripMargin)
        tempSaAccessDF.createOrReplaceTempView("temp_sa_access")
        val saAccessWrongIpDF = spark.sql(
            s"""
               |select
               |time,
               |adslocation_id,
               |user_ip,
               |if(impression >20 ,1,0) as condition1,
               |if( impression > 5 and (resp * 1.0 )/impression  < 0.5 ,1,0) as condition2,
               |if(clicks > 3,1,0) as condition3,
               |if(impression >20 or ( impression > 5 and (resp * 1.0 )/(impression * 1.0) < 0.5 ) ,impression,null) as impression,
               |if( impression > 5 and (resp * 1.0 )/(impression * 1.0) < 0.5  ,resp,null ) as resp,
               |if(clicks=0,null,clicks) as clicks
               |from
               |(
               |
               |select
               |'$etlDate' as `time`,
               |adslocation_id,
               |user_ip,
               |sum(impression) as impression,
               |sum(resp) as resp,
               |max(clicks) as clicks  -- 每天每小时最大的
               |from
               |(
               |
               |select
               |hour,  -- 小时
               |adslocation_id,
               |user_ip,
               |sum(if(call='call',1,0)) as impression, -- 曝光数
               |sum(if(call='resp',1,0)) as resp, --未返回数
               |sum(if(call='clk',1,0)) as clicks --点击次数
               |from
               |temp_sa_access
               |group by adslocation_id,user_ip,hour
               |
               |) t
               |group by adslocation_id,user_ip
               |
               |) t
               |where impression >20
               |or ( impression > 5 and (resp * 1.0 )/(impression * 1.0) < 0.5 )
               |or (clicks > 3)
               |
               |""".stripMargin)
            println(s"一天内ip异常的数目${saAccessWrongIpDF.count()}")
            saAccessWrongIpDF.show()
        saAccessWrongIpDF.createOrReplaceTempView("saAccessWrongIp")

        val saAccessWrongIpDF2=spark.sql(
            s"""
               |select
               |a.time,
               |b.tags_map_id as adslocation_id,
               |a.user_ip,
               |b.url_id,
               |b.channel_id,
               |a.condition1,
               |a.condition2,
               |a.condition3,
               |a.impression,
               |a.resp,
               |a.clicks
               |from
               |saAccessWrongIp a
               |inner join
               |tagsMapLink b
               |on a.adslocation_id= b.tags_md5
               |""".stripMargin)
        saAccessWrongIpDF2.write
            .mode("append")
            .format("jdbc")
            .option("url", url)
            .option("driver", driver)
            .option("user", user)
            .option("password", password)
            .option("dbtable", saAccessWrongIpTable)
            .save()

        /*
任务三：以天为维度按域名、渠道ID统计包名情况；

表结构示例：

日期、域名、渠道ID、包名、统计数
 */ val saAccessCountDF = spark.sql(
            s"""
               |select
               |a.time,
               |coalesce(b.id,a.domain) as domain,
               |coalesce(c.id,a.chid) as chid,
               |a.package_name,
               |a.cnt
               |from
               |(
               |select
               |    '${etlDate}' as time,
               |     domain,
               |     chid,
               |    package_name,
               |    count(1) as cnt
               |from
               |(
               |select
               |
               |    str_map['domain'] as domain,
               |    str_map['chid'] as chid,
               |    package_name
               |from
               |(
               |select
               |to_map(request) as str_map,
               |package_name
               |from
               |ods.ods_sa_access
               |where etl_date='$etlDate' and length(package_name)<=200
               |) t
               |) t
               |group by domain,chid,package_name
               |
               |) a
               |left join
               |url b
               |on a.domain = b.url
               |left join
               |channelIdMap c
               |on a.chid= c.channel_id
               |
               |""".stripMargin)
        saAccessCountDF.show()
        println("count:=="+saAccessCountDF.count())
        saAccessCountDF.write.mode("append")
            .format("jdbc")
            .option("url", url)
            .option("driver", driver)
            .option("user", user)
            .option("password", password)
            .option("dbtable", saAccessCountTable)
            .save()


    }



        /*    val statDF = spark.sql(
      """
        |select	a.time,
        |		a.hour,
        |		a.impression,
        |		a.resp,
        |		a.clicks,
        |		a.template_id,
        |		a.page,
        |		a.adflag,
        |		b.tags_map_id,
        |		b.link_id ,
        |		b.url_id,
        |		b.user_id,
        |		b.station_id,
        |		c.id as channel_id_map_id
        |from
        |(
        |	select 	time,
        |			hour,
        |			sum(impression) impression,
        |			sum(resp) resp,
        |			sum(clicks) clicks,
        |			chid,
        |			template_id,
        |			page,
        |			adflag,
        |			adslocation_id
        |	from 	saAccess
        |	group by time,hour,chid,template_id,page,adflag,adslocation_id
        |) a
        |inner join tagsMapLink b
        |on a.adslocation_id = b.tags_md5
        |inner join  channelIdMap c
        |on a.chid = c.channel_id
        |""".stripMargin)
    // chid  这里有点问题，日志字段顺序不一样，有部分chid取值不对，估先用inner join
    statDF.write.mode("append")
      .format("jdbc")
      .option("url",url)
      .option("driver",driver)
      .option("user",user)
      .option("password",password)
      .option("dbtable",statTable)
      .save()

  }*/
        //大任务二==========================================
        ///data/logs/access_2020-06-19.log
        /*
uri 以"/vip{数字}.html" 开头
忽略uri参数，按天统计出请求总数；

uri 以"/api/v1/sa"开头
忽略uri参数，按天统计出请求总数；

其他链接，正常统计。
 */
        def readAccess(spark: SparkSession, args: Array[String]): Unit = {

            val patternVip = "^/vip\\d*".r

            spark.udf.register("get_uri", (s: String) => {
                var res:String = null
                val arr = s.split(" +")
                if (arr.length == 3) {
                    val uri = arr(1)
                    if (uri.matches("^/api/v1/sa.*")) {
                        res= "/api/v1/sa"
                    }else {
                        patternVip.findFirstIn(uri) match {
                            case Some(s) => res=s
                            case None => res=uri
                        }
                    }
                } //脏数据
                res
            })



            val etlDate = args(0)

            val sdf = new SimpleDateFormat("yyyy-MM-dd")

            val calendar=Calendar.getInstance()
            calendar.setTime(sdf.parse(etlDate))
            calendar.set(Calendar.HOUR_OF_DAY,-24);
            val yesterdayDate=sdf.format(calendar.getTime)

//            val accessDF = spark.sql(
//                s"""
//                    |select
//                    |'${etlDate}' as time,
//                    |hostname,
//                    |concat(if(hostname is null,'',hostname),if(url is null,'',url)) as url,
//                    |count(1) as request_num
//                    |from
//                    |(
//                    |select
//                    |hostname ,
//                    |get_uri(request) as url
//                    |from
//                    |ods.ods_access
//                    |where etl_date = '$etlDate' and length(hostname) <=200
//                    |) t
//                    |group by hostname,url
//                    |
//                    |""".stripMargin)
            val accessDF = spark.sql(
                    s"""
                       |select
                       |t1.time,
                       |coalesce(t1.hostname,t2.hostname) as hostname,
                       |coalesce(t1.url,t2.url) as url,
                       |nvl(t1.request_num,0) as request_num,
                       |nvl(t2.request_num,0) as pre_request_num,
                       |nvl(t1.request_num,0) -nvl(t2.request_num,0) as change_num,
                       |round(if(t2.request_num is null ,999999,if(t2.request_num <1000 ,null,(nvl(t1.request_num,0)-t2.request_num) * 1.0/t2.request_num)),2) as change_rate
                       |
                       |from
                       |(
                       |    select
                       |    '$etlDate' as time,
                       |    hostname,
                       |    concat(if(hostname is null,'',hostname),if(url is null,'',url)) as url,
                       |    count(1) as request_num
                       |    from
                       |    (
                       |    select
                       |    hostname ,
                       |    get_uri(request) as url
                       |    from
                       |    ods.ods_access
                       |    where etl_date = '$etlDate' and length(hostname) <=200
                       |    ) t
                       |    group by hostname,url
                       |) t1
                       |left join
                       |(
                       |    select
                       |    hostname,
                       |    concat(if(hostname is null,'',hostname),if(url is null,'',url)) as url,
                       |    count(1) as request_num
                       |    from
                       |    (
                       |    select
                       |    hostname ,
                       |    get_uri(request) as url
                       |    from
                       |    ods.ods_access
                       |    where etl_date = '$yesterdayDate' and length(hostname) <=200
                       |    ) t
                       |    group by hostname,url
                       |) t2
                       |on t1.url = t2.url and t1.hostname = t2.hostname
                       |where
                       | ( t1.request_num is not null or (t1.request_num is null and t2.request_num > 1000) ) and t1.time is not null
                       |
                       |""".stripMargin)

//            accessDF.show()
            accessDF.write.mode("append")
                .format("jdbc")
                .option("url", url)
                .option("driver", driver)
                .option("user", user)
                .option("password", password)
                .option("dbtable", AccessTable)
                .save()
        }
}
