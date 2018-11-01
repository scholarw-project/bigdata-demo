package org.wnsg.storm.demo.test;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.wnsg.storm.demo.test.bolts.TestBolt;
import org.wnsg.storm.demo.test.spouts.TestSpout;

public class TestTopologyMain {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// 1、准备一个 TopologyBuilder
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("testSpout", new TestSpout(), 1);
		topologyBuilder.setBolt("testBolt", new TestBolt(), 10).shuffleGrouping("mySpout");

		// 2、创建configuration,指定topology 需要的worker的数量
		Config config = new Config();
		config.setNumWorkers(2);

		// 3、提交任务，分为本地模式、集群模式
		// StormSubmitter.submitTopologyWithProgressBar("mywordcount",config,topologyBuilder.createTopology());

		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("testTopology", config, topologyBuilder.createTopology());
	}

}
