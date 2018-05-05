package com.yq.stream;

/**
 * Simple to Introduction
 * className: SpilterFunction
 *
 * @author EricYang
 * @version 2018/5/5 19:56
 */
import com.yq.spouts.WordReader;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpilterFunction extends BaseFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(SpilterFunction.class);


    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String sentens = tuple.getString(0);
        String[] array = sentens.split("\\s+");
        for(int i=0;i<array.length;i++){
            System.out.println("spilter emit:" + array[i]);
            log.info("spilter emit:" + array[i]);
            collector.emit(new Values(array[i]));
        }
    }

}