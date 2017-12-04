package cn.cs.scu.dao.implement;

import cn.cs.scu.constants.Constants;
import cn.cs.scu.dao.DaoImplement;
import cn.cs.scu.domain.Ad;
import cn.cs.scu.javautils.SqlUtils;
import cn.cs.scu.jdbc.JDBCHelper;
import org.json.JSONException;
import org.json.JSONObject;

import java.sql.PreparedStatement;
import java.util.ArrayList;


/**
 * 广告表数据访问对象实现类
 * <p>
 * Created by Wanghan on 2017/3/15.
 * Copyright © Wanghan SCU. All Rights Reserved
 */
public class AdDaoImplement extends DaoImplement {
    //测试
    public static void main (String[] args){

        String updatetable_sql = "INSERT INTO " + Constants.TABLE_AD + "(" + Constants.FIELD_AD_ID + ","
                + Constants.FIELD_CLICK_DAY + "," + Constants.FIELD_CLICK_TIME + "," +
                Constants.FIELD_CLICK_NUMBER + ") VALUE(?,?,?,?) ON DUPLICATE KEY UPDATE " + Constants.FIELD_CLICK_NUMBER +
                "=?";
        String gettable_sql = "SELECT * FROM " + Constants.TABLE_AD;
        System.out.println("updatetable_sql: "+updatetable_sql+"\n"+"gettable_sql: "+gettable_sql);
        System.out.println("SELECT ad_id,click_day,CONCAT(SUBSTRING(click_time, 1, 5), \":00\") AS oneTime,SUM(click_number) FROM " +
                Constants.TABLE_AD + " WHERE (click_day BETWEEN ? AND ?) AND (click_time BETWEEN ? AND ?)" +
                " GROUP BY ad_id, click_day,oneTime");

    }
    /**
     * 更新广告表
     *
     * @param ads
     */
    @Override
    public void updateTable(Object[] ads) {
        if (ads instanceof Ad[]) {
            // jdbc单例
            JDBCHelper jdbcHelper = JDBCHelper.getInstanse();
            String sql = "INSERT INTO " + Constants.TABLE_AD + "(" + Constants.FIELD_AD_ID + ","
                    + Constants.FIELD_CLICK_DAY + "," + Constants.FIELD_CLICK_TIME + "," +
                    Constants.FIELD_CLICK_NUMBER + ") VALUE(?,?,?,?) ON DUPLICATE KEY UPDATE " + Constants.FIELD_CLICK_NUMBER +
                    "=?";

            // sql批量插入数据
            jdbcHelper.excuteInsert(sql, ads, (String sql1, PreparedStatement preparedStatement, Object[] objects) -> {
                for (Ad ad : (Ad[]) objects) {
                    preparedStatement.setObject(1, ad.getAdId());
                    preparedStatement.setObject(2, ad.getClickDay());
                    preparedStatement.setObject(3, ad.getClickTime());
                    preparedStatement.setObject(4, ad.getClickNumber());
                    preparedStatement.setObject(5, ad.getClickNumber());
                    preparedStatement.addBatch();
                }
                // 批量插入
                preparedStatement.executeBatch();
            });
        }
    }

    /**
     * 获得广告表符合条件的数据
     * 注意，这里广告表里并没有开始点击时间，结束点击时间等列
     * @param param
     * @return
     */

    @Override//为什么要传参进去？？？
    public Object[] getTable(JSONObject param) {
        String sql = "SELECT * FROM " + Constants.TABLE_AD;
        Long ad_id = null;
        String ad_type = null;
        String start_click_day = null;
        String end_click_day = null;
        String start_click_time = null;
        String end_click_time = null;
        Long click_number = null;

        // 查询语句参数
        ArrayList<Object> paramLists = new ArrayList<>();//存放传入的json对象的各属性

        try {
            ad_id = param.getLong(Constants.FIELD_AD_ID);
            String currentSql = Constants.FIELD_AD_ID + " =?";
            paramLists.add(ad_id);
            sql = SqlUtils.concatSQL(sql, currentSql);//将传入对象的字段属性添加为where子句
        } catch (JSONException e) {
            System.out.println("查询广告表数据时，传入的json对象, key: ad_id doesn't exist");
        }

        try {
            ad_type = param.getString(Constants.FIELD_AD_TYPE);
            String currentSql = Constants.FIELD_AD_TYPE + " =?";
            paramLists.add(ad_type);
            sql = SqlUtils.concatSQL(sql, currentSql);//将传入对象的字段属性添加为where子句的and 条件
        } catch (JSONException e) {
            System.out.println("查询广告表数据时，传入的json对象, key: ad_type doesn't exist");
        }

        try {
            start_click_day = param.getString(Constants.FIELD_START_CLICK_DAY);
            String currentSql = Constants.FIELD_START_CLICK_DAY + " >=?";
            paramLists.add(start_click_day);
            sql = SqlUtils.concatSQL(sql, currentSql);
        } catch (JSONException e) {
            System.out.println("查询广告表数据时，传入的json对象, key: start_click_day doesn't exist");
        }

        try {
            end_click_day = param.getString(Constants.FIELD_END_CLICK_DAY);
            String currentSql = Constants.FIELD_END_CLICK_DAY + " <=?";
            paramLists.add(end_click_day);
            sql = SqlUtils.concatSQL(sql, currentSql);
        } catch (JSONException e) {
            System.out.println("查询广告表数据时，传入的json对象, key: end_click_day doesn't exist");
        }

        try {
            start_click_time = param.getString(Constants.FIELD_START_CLICK_TIME);
            String currentSql = Constants.FIELD_START_CLICK_TIME + " >=?";
            paramLists.add(start_click_time);
            sql = SqlUtils.concatSQL(sql, currentSql);
        } catch (JSONException e) {
            System.out.println("查询广告表数据时，传入的json对象, key: start_click_time doesn't exist");
        }

        try {
            end_click_time = param.getString(Constants.FIELD_END_CLICK_TIME);
            String currentSql = Constants.FIELD_END_CLICK_TIME + " <=?";
            paramLists.add(end_click_time);
            sql = SqlUtils.concatSQL(sql, currentSql);
        } catch (JSONException e) {
            System.out.println("查询广告表数据时，传入的json对象, key: end_click_time doesn't exist");
        }

        try {
            click_number = param.getLong(Constants.FIELD_CLICK_NUMBER);
            String currentSql = Constants.FIELD_CLICK_NUMBER + " =?";
            paramLists.add(click_number);
            sql = SqlUtils.concatSQL(sql, currentSql);
        } catch (JSONException e) {
            System.out.println("查询广告表数据时，传入的json对象, key: click_number doesn't exist");
        }

        // jdbc单例
        JDBCHelper jdbcHelper = JDBCHelper.getInstanse();
        ArrayList<Ad> ads = new ArrayList<>();
        jdbcHelper.executeQuery(sql, paramLists.toArray(), rs -> {
            while (rs.next()) {
                Ad ad = new Ad();
                ad.setAdId(rs.getLong(1));
                ad.setAdType(rs.getString(2));
                ad.setClickDay(rs.getString(3));
                ad.setClickTime(rs.getString(4));
                ad.setClickNumber(rs.getLong(5));
                ads.add(ad);
            }
        });
        return ads.toArray(new Ad[0]).clone();
    }

    /**
     * 一小时内广告点击数
     *
     * @param param
     * @return
     */
    public Ad[] getOneHourAdClick(JSONObject param) {
        String sql = "SELECT ad_id,click_day,CONCAT(SUBSTRING(click_time, 1, 5), \":00\") AS oneTime,SUM(click_number) FROM " +
                Constants.TABLE_AD + " WHERE (click_day BETWEEN ? AND ?) AND (click_time BETWEEN ? AND ?)" +
                " GROUP BY ad_id, click_day,oneTime";
        String start_click_day = param.getString(Constants.FIELD_START_CLICK_DAY);
        String end_click_day = param.getString(Constants.FIELD_END_CLICK_DAY);
        String start_click_time = param.getString(Constants.FIELD_START_CLICK_TIME);
        String end_click_time = param.getString(Constants.FIELD_END_CLICK_TIME);
        Object[] params = {start_click_day, end_click_day, start_click_time, end_click_time};
        // jdbc单例
        JDBCHelper jdbcHelper = JDBCHelper.getInstanse();
        ArrayList<Ad> ads = new ArrayList<>();
        jdbcHelper.executeQuery(sql, params, rs -> {
            while (rs.next()) {
                Ad ad = new Ad();
                ad.setAdId(rs.getLong(1));
                ad.setClickDay(rs.getString(2));
                ad.setClickTime(rs.getString(3));
                ad.setClickNumber(rs.getLong(4));
                ads.add(ad);
            }
        });
        return ads.toArray(new Ad[0]);
    }
}
