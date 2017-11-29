package cn.cs.scu.dao.implement;

import cn.cs.scu.constants.Constants;
import cn.cs.scu.dao.DaoImplement;
import cn.cs.scu.domain.Blacklist;
import cn.cs.scu.javautils.SqlUtils;
import cn.cs.scu.jdbc.JDBCHelper;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;

/**
 * 黑名单类数据访问对象实现类
 * <p>
 * Created by Wanghan on 2017/3/15.
 * Copyright © Wanghan SCU. All Rights Reserved
 */
public class BlacklistDaoImplement extends DaoImplement {

    /**
     * 更新黑名单表
     *z
     * @param blacklists
     */
    @Override
    public void updateTable(Object[] blacklists) {
        if (blacklists instanceof Blacklist[]) {
            // jdbc单例
            JDBCHelper jdbcHelper = JDBCHelper.getInstanse();
            // 如果key不存在则插入，如果存在则更新
            String sql = "INSERT IGNORE INTO " + Constants.TABLE_BLACKLIST + "(" + Constants.FIELD_USER_ID + "," +
                    Constants.FIELD_USER_NAME + ") VALUE(?,?)";

            // sql批量插入数据
            jdbcHelper.excuteInsert(sql, blacklists, (sql1, preparedStatement, objects) -> {
                for (Blacklist blacklist : (Blacklist[]) blacklists) {
                    preparedStatement.setObject(1, blacklist.getUser_id());
                    preparedStatement.setObject(2, blacklist.getUser_name());

                    preparedStatement.addBatch();
                }
                // 批量插入
                preparedStatement.executeBatch();
            });
        }
    }

    /**
     * 获取黑名单表
     *
     * @param param
     * @return
     */
    @Override
    public Object[] getTable(JSONObject param) {
        String sql = "SELECT * FROM " + Constants.TABLE_BLACKLIST;
        Long user_id = null;
        String user_name = null;
        // 查询语句参数
        ArrayList<Object> paramLists = new ArrayList<>();

        try {
            user_id = param.getLong(Constants.FIELD_USER_ID);
            String currentSql = Constants.FIELD_USER_ID + " =?";
            paramLists.add(user_id);
            sql = SqlUtils.concatSQL(sql, currentSql);
        } catch (JSONException e) {
            System.out.println("key: user_id doesn't exist");
        }

        try {
            user_name = param.getString(Constants.FIELD_USER_NAME);
            String currentSql = Constants.FIELD_USER_NAME + " =?";
            paramLists.add(user_name);
            sql = SqlUtils.concatSQL(sql, currentSql);
        } catch (JSONException e) {
            System.out.println("key: user_name doesn't exist");
        }

        // jdbc单例
        JDBCHelper jdbcHelper = JDBCHelper.getInstanse();
        ArrayList<Blacklist> blacklists = new ArrayList<>();


        jdbcHelper.executeQuery(sql, paramLists.toArray(), rs -> {
            while (rs.next()) {
                Blacklist blacklist = new Blacklist();
                blacklist.setUser_id(rs.getString(1));
                blacklist.setUser_name(rs.getString(2));
                blacklists.add(blacklist);

            }

        });
        // 返回黑名单数组对象的克隆，防止连接关闭后，数据被清空
        return blacklists.toArray(new Blacklist[0]).clone();
    }

}
