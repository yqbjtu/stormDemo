package com.yq;

import com.yq.bolts.CounterBolt;
import com.yq.bolts.MongoBolt;
import com.yq.stream.PrintFilterPartition;
import com.yq.stream.SpilterFunction;
import com.yq.stream.SumWord;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.Broker;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;

import java.util.Arrays;
import java.util.UUID;

/**
 * Simple to Introduction
 * className: KafkaStormDemo
 *
 * @author yqbjtu
 * @version 2018/4/26 11:30
 */
public class KafkaStormV2Demo {
    public static void main(String[] args) throws Exception{

        String zkUrl = "127.0.0.1:2181";
        if (args != null && args.length > 1) {
            System.out.println("args[0]:" + args[0]);
            System.out.println("args[1]:" + args[1]);
            zkUrl = args[0];
        }

        String topic = "test";
        System.out.println("kafkaSpoutConfig setting done.");


        TridentTopology topology = new TridentTopology();
        ZkHosts zkHosts = new ZkHosts(zkUrl);
        TridentKafkaConfig tridentKafkaConfig = new TridentKafkaConfig(zkHosts, topic, UUID.randomUUID().toString());
        tridentKafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        TransactionalTridentKafkaSpout transactionalTridentKafkaSpout = new TransactionalTridentKafkaSpout(
                tridentKafkaConfig);
        topology.newStream("name",transactionalTridentKafkaSpout)
                .shuffle()
                .each(new Fields("str"), new SpilterFunction(), new Fields("sentence"))
                .groupBy(new Fields("sentence"))
                .aggregate(new Fields("sentence"), new SumWord(),new Fields("sum"))
                .parallelismHint(5)
                .each(new Fields("sum"), new PrintFilterPartition());

        Config config = new Config();
        config.setDebug(true);
        //config.put(Config.NIMBUS_THRIFT_PORT, 6627);

        if (args != null && args.length > 1) {
            System.out.println("args[0]:" + args[0]);
            System.out.println("args[1]:" + args[1]);
            config.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[1], config, topology.build());
        } else if (args == null){
            System.out.println("args:" + args);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("KafkaStormV2Demo", config, topology.build());
            Thread.sleep(10000);
            cluster.killTopology("KafkaStormV2Demo");
            cluster.shutdown();
        }
    }
}
