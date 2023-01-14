package com.saransh.pardo;

import com.saransh.Analyzer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LevelExtracter extends DoFn<String, KV<String, String>> {
    private static final Logger log = LoggerFactory.getLogger(Analyzer.class);

    @ProcessElement
    public void processElement(ProcessContext c) {
        String line = c.element();
        final String regex = "\\d\\d\\d\\d-\\d\\d-\\d\\d [0-9]{1,2}:\\d\\d:\\d\\d,\\d\\d\\d [A-Z]{4,5} [a-zA-Z0-9.]+";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(line);
        if (matcher.find())
        {
            String matched = matcher.group(0);
            String[] collect = matched.split(" ");
            c.output(KV.of(collect[2], collect[3]));
        }



    }
}
