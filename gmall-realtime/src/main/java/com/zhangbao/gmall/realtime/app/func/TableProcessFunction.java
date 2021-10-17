package com.zhangbao.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.zhangbao.gmall.realtime.bean.TableProcess;
import com.zhangbao.gmall.realtime.common.GmallConfig;
import com.zhangbao.gmall.realtime.utils.MysqlUtil;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * @author: zhangbao
 * @date: 2021/8/26 23:24
 * @desc:
 **/
@Log4j2
public class TableProcessFunction extends ProcessFunction<JSONObject,JSONObject> {
    //定义输出流标记
    private OutputTag<JSONObject> outputTag;
    //定义配置信息
    private Map<String , TableProcess> tableProcessMap = new HashMap<>();
    //在内存中存放已经创建的表
    Set<String> existsTable = new HashSet<>();
    //phoenix连接对象
    Connection con = null;

    public TableProcessFunction(OutputTag<JSONObject> outputTag) {
        this.outputTag = outputTag;
    }

    //只执行一次
    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化配置信息
        log.info("查询配置表信息");
        //创建phoenix连接
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        con = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
//        refreshDate();
        //启动一个定时器，每隔一段时间重新获取配置信息
        //delay：延迟5000执行，每隔5000执行一次
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                refreshDate();
            }
        },5000,5000);
    }



    //每进来一个元素，执行一次
    @Override
    public void processElement(JSONObject jsonObj, Context context, Collector<JSONObject> collector) throws Exception {
        //获取表的修改记录
        String table = jsonObj.getString("table");
        String type = jsonObj.getString("type");
        JSONObject data = jsonObj.getJSONObject("data");
        if(type.equals("bootstrap-insert")){
            //maxwell更新历史数据时，type类型是bootstrap-insert
            type = "insert";
            jsonObj.put("type",type);
        }
        if(tableProcessMap != null && tableProcessMap.size()>0){
            String key = table + ":" + type;
            TableProcess tableProcess = tableProcessMap.get(key);
            if(tableProcess!=null){
                //数据发送到何处，如果是维度表，就发送到hbase，如果是事实表，就发送到kafka
                String sinkTable = tableProcess.getSinkTable();
                jsonObj.put("sink_table",sinkTable);
                String sinkColumns = tableProcess.getSinkColumns();
                //过滤掉不要的数据列，sinkColumns是需要的列
                filterColumns(data,sinkColumns);

            }else {
                log.info("no key {} for mysql",key);
            }
            if(tableProcess!=null && tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_HBASE)){
                //根据sinkType判断，如果是维度表就分流，发送到hbase
                context.output(outputTag,jsonObj);
            }else if(tableProcess!=null && tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_KAFKA)){
                //根据sinkType判断，如果是事实表就发送主流，发送到kafka
                collector.collect(jsonObj);
            }

        }
    }

    //过滤掉不要的数据列，sinkColumns是需要的列
    private void filterColumns(JSONObject data, String sinkColumns) {
        String[] cols = sinkColumns.split(",");
        //转成list集合，用于判断是否包含需要的列
        List<String> columnList = Arrays.asList(cols);
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        while (iterator.hasNext()){
            Map.Entry<String, Object> next = iterator.next();
            String key = next.getKey();
            //如果不包含就删除不需要的列
            if(!columnList.contains(key)){
                iterator.remove();
            }
        }
    }

    //读取配置信息，并创建表
    private void refreshDate() {
        List<TableProcess> processList = MysqlUtil.getList("select * from table_process", TableProcess.class, true);
        for (TableProcess tableProcess : processList) {
            log.info("获取配置信息- > {}",tableProcess.toString());
            String sourceTable = tableProcess.getSourceTable();
            String operateType = tableProcess.getOperateType();
            String sinkType = tableProcess.getSinkType();
            String sinkTable = tableProcess.getSinkTable();
            String sinkColumns = tableProcess.getSinkColumns();
            String sinkPk = tableProcess.getSinkPk();
            String sinkExtend = tableProcess.getSinkExtend();
            String key = sourceTable+":"+operateType;
            tableProcessMap.put(key,tableProcess);
            //在phoenix创建表
            if(TableProcess.SINK_TYPE_HBASE.equals(sinkType) && operateType.equals("insert")){
                boolean noExist = existsTable.add(sinkTable);//true则表示没有创建表
                if(noExist){
                    createTable(sinkTable,sinkColumns,sinkPk,sinkExtend);
                }
            }
        }
    }

    //在phoenix中创建表
    private void createTable(String table, String columns, String pk, String ext) {
        if(StringUtils.isBlank(pk)){
            pk = "id";
        }
        if(StringUtils.isBlank(ext)){
            ext = "";
        }
        StringBuilder sql = new StringBuilder("create table if not exists " + GmallConfig.HBASE_SCHEMA + "." + table +"(");
        String[] split = columns.split(",");
        for (int i = 0; i < split.length; i++) {
            String field = split[i];
            if(pk.equals(field)){
                sql.append(field + " varchar primary key ");
            }else {
                sql.append("info." + field +" varchar ");
            }
            if(i < split.length-1){
                sql.append(",");
            }
        }
        sql.append(")").append(ext);
        //创建phoenix表
        PreparedStatement ps = null;
        try {
            log.info("创建phoenix表sql - >{}",sql.toString());
            ps = con.prepareStatement(sql.toString());
            ps.execute();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }finally {
            if(ps!=null){
                try {
                    ps.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                    throw new RuntimeException("创建phoenix表失败");
                }
            }
        }
        if(tableProcessMap == null || tableProcessMap.size()==0){
            throw new RuntimeException("没有从配置表中读取配置信息");
        }
    }
}
