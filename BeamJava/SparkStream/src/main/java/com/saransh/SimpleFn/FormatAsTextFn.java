package com.saransh.SimpleFn;

import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

public class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
    @Override
    public String apply(KV<String, Long> input) {
        return "<tr><td>" + input.getKey() + "</td><td>" + input.getValue() +"</td></tr>";

    }
}
