package cn.cs.scu.constants;
import cn.cs.scu.conf.ConfigurationManager;
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

    String JDBC_DRIVER = "jdbc.driver";
    String JDBC_URL = "jdbc.url";
    String JDBC_USER = "jdbc.user";
    String JDBC_PASSWORD = "jdbc.password";
    String JDBC_USER_PROD = "";
    String JDBC_URL_PROD = "";
    String JDBC_PASSWORD_PROD = "";
    String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
    String JDBC_HOST="jdbc.host";

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

    String CHECK_POINT_DIR = "/home/yangzhaoyunfei/temp";
//    String KAFKA_ZKQUORUM = "wfbs";
//    String KAFKA_GROUP = "g1";
//    String KAFKA_TOPICS = "kafka.topics";
//    String CLICK_TIMES = "click.times";
//    String THREADS_NUM = "threads.num";
//    String USERS_NUM = "users.num";
//    String ADS_NUM = "ads.num";
    Integer ADS_NUM = ConfigurationManager.getInteger("ads.num");
    Integer USERS_NUM = ConfigurationManager.getInteger("users.num");
    Integer THREADS_NUM = ConfigurationManager.getInteger("threads.num");
    Integer CLICK_TIMES = ConfigurationManager.getInteger("click.times");
    String KAFKA_TOPICS = ConfigurationManager.getString("kafka.topics");
    String KAFKA_GROUP = ConfigurationManager.getString("kafka.group");
    String KAFKA_ZKQUORUM = ConfigurationManager.getString("kafka.zkquorum");

}
