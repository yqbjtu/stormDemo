package com.yq.spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

public class WordReader implements IRichSpout {
    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private boolean completed = false;
    private TopologyContext context;
    private static final Logger log = LoggerFactory.getLogger(WordReader.class);

    public boolean isDistributed() {
        return false;
    }

    @Override
    public void ack(Object msgId) {
        System.out.println("OK:" + msgId);
    }

    public void close() {
    }

    public void activate() {

    }

    public void deactivate() {

    }

    @Override
    public void fail(Object msgId) {
        System.out.println("FAIL:" + msgId);
    }

    /** step:2
     * 这个方法做的就是分发文件中的文本行
     */
    @Override
    public void nextTuple() {
        /**
         * 这个方法会不断的被调用，直到整个文件都读完了，我们将等待并返回。
         */
        if (completed) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // 什么也不做
            }
            return;
        }
        String str;

        try {
            // 创建reader
//            BufferedReader reader = new BufferedReader(fileReader);
            // 读所有文本行
//            while ((str = reader.readLine()) != null) {
//                /**
//                 * 按行发布一个新值
//                 */
//                this.collector.emit(new Values(str), str);
//            }
            str = "it can hurt you. If you all disagree the whole time and insist on" +
                    " going your separate ways, the first enemy you meet will be able to destroy you.";
            this.collector.emit(new Values(str), str);
            log.info("nextTuple read:" + str);
        } catch (Exception e) {
            throw new RuntimeException("Error reading tuple", e);
        } finally {
            completed = true;
        }
    }

    /** step:1
     * 我们将创建一个文件并维持一个collector对象
     */
    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.context = context;
//        try {
//
//            this.fileReader = new FileReader(conf.get("wordsFile").toString());
//        } catch (FileNotFoundException e) {
//            throw new RuntimeException("Error reading file ["
//                    + conf.get("wordFile") + "]");
//        }
        log.info("WordReader read:" );
        this.collector = collector;
    }

    /**
     * 声明输入域"word"
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}