/*
 * Copyright (C) 2018 org.citic.iiot, Inc. All Rights Reserved.
 */


package com.yq.stream;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple to Introduction
 * className: PrintFilterPartition
 *
 * @author EricYang
 * @version 2018/5/5 20:00
 */

public class PrintFilterPartition extends BaseFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(PrintFilterPartition.class);

    private static final long serialVersionUID = 1L;

    @Override
    public boolean isKeep(TridentTuple tuple) {
        LOGGER.info("打印出来的tuple:" + tuple);
        return true;
    }
}