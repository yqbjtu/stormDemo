package com.yq;

/**
 * Simple to Introduction
 * className: TopologyMain
 *
 * @author yqbjtu
 * @version 2018/4/25 11:42
 */

import com.yq.bolts.WordCounter;
import com.yq.bolts.WordNormalizer;
import com.yq.spouts.WordReader;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.util.Arrays;

public class TopologyMain {
    public static void main(String[] args) throws Exception {
        //定义拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader());
        builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter()).fieldsGrouping("word-normalizer", new Fields("word"));

        //配置
        Config conf = new Config();
        System.out.println("args[0]:" + args[0]);
        conf.put("wordsFile", args[0]);
//        //10.4.16.58 ubuntu01
//        conf.put(Config.NIMBUS_SEEDS,"ubuntu01");//配置nimbus连接端口，默认 6627
//        conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("ubuntu01")); //配置zookeeper连接主机地址，可以使用集合存放多个
//        conf.put(Config.STORM_ZOOKEEPER_PORT, 2181); //配置zookeeper连接端口，默认2181
        conf.setDebug(false);
        conf.setNumWorkers(1);

        //非常关键的一步，使用StormSubmitter提交拓扑时，不管怎么样，都是需要将所需的jar提交到nimbus上去，如果不指定jar文件路径，
        //storm默认会使用System.getProperty("storm.jar")去取，如果不设定，就不能提交
        //System.setProperty("storm.jar","d:\\storm-remote-submit-1.0-SNAPSHOT-jar-with-dependencies.jar");
        //StormSubmitter.submitTopology(name, conf, topology);
        if (args != null && args.length > 1) {
            conf.setNumWorkers(1);
            StormSubmitter.submitTopologyWithProgressBar(args[1], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            StormTopology topology = builder.createTopology();
            cluster.submitTopology("Getting-Started-Topologie", conf, topology);
            Utils.sleep(40000);
            cluster.killTopology("Getting-Started-Topologie");
            cluster.shutdown();
        }
    }
}