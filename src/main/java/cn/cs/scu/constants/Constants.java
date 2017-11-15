package cn.cs.scu.constants;

/**
 * 配置常量接口类，便于扩展
 * <p>
 * Created by Wanghan on 2017/3/15.
 * Copyright © Wanghan SCU. All Rights Reserved
 */

public interface Constants {
    //定义本地运行的线程数量
    String SPARK_MASTER = "local[4]";
    String SPARK_LOCAL = "spark.local";
    String SPARK_APP_NAME_SESSION = "AdRealTimeAnalysis";
    String SPARK_STREAMING_COLLECT_TIME = "spark.streaming.collect.time";

    String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    String JDBC_URL = "localhost:3306";
    String JDBC_USER = "root";
    String JDBC_PASSWORD = "root";
    String JDBC_USER_PROD = "";
    String JDBC_URL_PROD = "";
    String JDBC_PASSWORD_PROD = "";
    String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";

    String TABLE_AD = "ad";
    String TABLE_BLACKLIST = "blacklist";
    String TABLE_PROVINCE_CLICK = "province_click";
    String TABLE_USER = "user";
    String TABLE_USER_CLICK = "user_click";

    String FIELD_USER_ID = "user_id";
    String FIELD_USER_NAME = "user_name";

    String FIELD_AD_ID = "ad_id";
    String FIELD_AD_TYPE = "ad_type";
    String FIELD_CLICK_DAY = "click_day";
    String FIELD_START_CLICK_DAY = "start_click_day";
    String FIELD_END_CLICK_DAY = "end_click_day";
    String FIELD_CLICK_TIME = "click_time";
    String FIELD_START_CLICK_TIME = "start_click_time";
    String FIELD_END_CLICK_TIME = "end_click_time";
    String FIELD_CLICK_NUMBER = "click_number";
    String FIELD_PROVINCE = "province";
    String FIELD_CITY = "city";
    String FIELD_DATE = "date";
    String FIELD_USERID = "userId";
    String FIELD_ADID = "adId";

    String CHECK_POINT_DIR = "/Users/zhangchi/temp";
    String KAFKA_ZKQUORUM = "localhost";
    String KAFKA_GROUP = "g1";
    String KAFKA_TOPICS = "tttt";
    Integer CLICK_TIMES = 100;
    Integer THREADS_NUM = 3;
    Integer USERS_NUM = 1000;
    Integer ADS_NUM= 10;


}
