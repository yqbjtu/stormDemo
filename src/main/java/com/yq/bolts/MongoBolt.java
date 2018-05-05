package com.yq.bolts;

import com.yq.spouts.WordReader;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Simple to Introduction
 * className: WordCounter
 *
 * @author yqbjtu
 * @version 2018/4/25 11:42
 */

public class MongoBolt extends BaseBasicBolt {
    private static final Logger log = LoggerFactory.getLogger(MongoBolt.class);
    String name;
    Integer id;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String word = tuple.getString(0);

        collector.emit(new Values(word));
        System.out.println("MongoBolt(will write to mongo) execute:" + word );
    }

    /**
     * 这个spout结束时（集群关闭的时候），我们会显示单词数量
     */
    @Override
    public void cleanup() {
        System.out.println("-- MongoBolt  cleanup --");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }




}