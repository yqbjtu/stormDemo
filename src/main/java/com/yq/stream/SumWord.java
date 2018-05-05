
package com.yq.stream;

/**
 * Simple to Introduction
 * className: SumWord
 *
 * @author EricYang
 * @version 2018/5/5 19:58
 */
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class SumWord extends BaseAggregator<Map<String,Integer>> {

    private static final long serialVersionUID = 1L;

    /**
     * 属于哪个batch
     */
    private Object batchId;

    /**
     * 属于哪个分区
     */
    private int partitionId;

    /**
     * 分区数量
     */
    private int numPartitions;

    /**
     * 用来统计
     */
    private Map<String,Integer> state;


    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        state = new HashMap<String,Integer>();
        partitionId = context.getPartitionIndex();
        numPartitions = context.numPartitions();
    }
    @Override
    public Map<String, Integer> init(Object batchId, TridentCollector collector) {
        this.batchId = batchId;
        return state;
    }
    @Override
    public void aggregate(Map<String, Integer> val, TridentTuple tuple,
                          TridentCollector collector) {
        System.out.println(tuple+";partitionId="+partitionId+";partitions="+numPartitions
                +",batchId:" + batchId);
        String word = tuple.getString(0);
        val.put(word, MapUtils.getInteger(val, word, 0)+1);
        System.out.println("sumWord:" + val);
    }
    @Override
    public void complete(Map<String, Integer> val, TridentCollector collector) {
        collector.emit(new Values(val));
    }

}
