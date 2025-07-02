package com.example.pekko.model;

import org.apache.pekko.cluster.ddata.ReplicatedData;

public class FxRate2 implements ReplicatedData {
    @Override
    public ReplicatedData merge(ReplicatedData that) {
        return this;
    }
}
