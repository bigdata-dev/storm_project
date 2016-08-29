package com.ryxc.storm.bolt;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.ryxc.storm.utils.MyDateUtils;
import com.ryxc.storm.utils.MyDbUtils;

import java.sql.Connection;
import java.sql.Statement;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogFilterBolt extends BaseRichBolt {
	private OutputCollector collector;
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}
    Pattern pattern = java.util.regex.Pattern.compile("页面下载成功\\.url:\\[http://[a-zA-Z0-9]+\\.(.*)/.*\\]\\.耗时\\[([0-9]+)\\]毫秒\\.当前时间戳:\\[([0-9]+)\\]");

    List<String> list = new ArrayList<String>();

    @Override
	public void execute(Tuple input) {
        try {
            if(input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)){
                //系统级别的tuple
                Connection connection = MyDbUtils.getConnection();
                Statement statement = connection.createStatement();
                for (String line : list
                        ) {
                    String[] split = line.split("--");
                    statement.addBatch("INSERT INTO LOG(topdomain,usetime,time) VALUES('" + split[0] + "','" + split[1] + "','" + MyDateUtils.formatDate2(new Date(Long.parseLong(split[2]))) + "')");
                }
                statement.executeBatch();
                System.out.println("成功批量入库数据："+list.size());
                list.clear();
                connection.close();
            }else{
                byte[] binaryByField = input.getBinaryByField("bytes");
                String value = new String(binaryByField);
                Matcher matcher = pattern.matcher(value);
                if(matcher.find()){
                    String topdomain = matcher.group(1);
                    String useTime = matcher.group(2);
                    String time = matcher.group(3);
                    list.add(topdomain+"--"+useTime+"--"+time);
                }
                this.collector.ack(input);
            }

		} catch (Exception e) {
			this.collector.fail(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
        HashMap<String, Object> map = new HashMap<String, Object>();
        map.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,3);
        return map;
    }
}
