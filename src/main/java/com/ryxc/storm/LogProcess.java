package com.ryxc.storm;

import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import com.ryxc.storm.bolt.LogFilterBolt;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class LogProcess {
	
	public static void main(String[] args) {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		BrokerHosts hosts = new ZkHosts("192.168.116.161:2181");//指定kafka的地址,在这通过zk的地址间接获取
		String topic = "spider";//kafka中创建的主题
		String zkRoot = "/kafkaspout";//其实就是在zk中指定一个根节点，后续会在这个节点中创建一个子节点保存storm消费kafka中数据的偏移量等信息
		String id = "123";//kafka的消费groupid
		SpoutConfig spoutConf = new SpoutConfig(hosts, topic, zkRoot, id);
		String SPOUT_ID = KafkaSpout.class.getSimpleName();
		String BOLT_ID1 = LogFilterBolt.class.getSimpleName();
		topologyBuilder.setSpout(SPOUT_ID, new KafkaSpout(spoutConf));
		topologyBuilder.setBolt(BOLT_ID1, new LogFilterBolt()).shuffleGrouping(SPOUT_ID);
		
		LocalCluster localCluster = new LocalCluster();
		String topology_name = LogProcess.class.getSimpleName();
        Config config = new Config();
        StormTopology topology = topologyBuilder.createTopology ();
        if(args.length==0){
            localCluster.submitTopology(topology_name, config, topology);
		}else{
            try {
                StormSubmitter.submitTopology(topology_name,config,topology);
            } catch (AlreadyAliveException e) {
                e.printStackTrace ();
            } catch (InvalidTopologyException e) {
                e.printStackTrace ();
            }
        }

		
	}

}
