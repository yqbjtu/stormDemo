package com.yq.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Simple to Introduction
 * className: WordCounter
 *
 * @author yqbjtu
 * @version 2018/4/25 11:42
 */

public class WordCounter extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();
    String name;
    Integer id;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String word = tuple.getString(0);
        Integer count = counts.get(word);
        if (count == null) {
            count = 0;
        }
        count++;
        counts.put(word, count);
        collector.emit(new Values(word, count));

    }

    /**
     * 这个spout结束时（集群关闭的时候），我们会显示单词数量
     */
    @Override
    public void cleanup() {
        System.out.println("-- 单词数 【" + name + "-" + id + "】 --");
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}