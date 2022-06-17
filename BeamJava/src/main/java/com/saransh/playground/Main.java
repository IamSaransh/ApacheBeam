package com.saransh.playground;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.apache.beam.sdk.transforms.Watch;



public class Main {

    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();
        Boolean watchGrowthTerminationCondition = true;
        PCollection<MatchResult.Metadata> collection1 = pipeline
                .apply("Read Incoming New files",
                        FileIO.match().filepattern("file-pattern")
                                .continuously(Duration.standardSeconds((watchGrowthTerminationCondition.equals(Boolean.TRUE)) ? 30 : 60),
                                        watchGrowthTerminationCondition.equals(Boolean.TRUE) ? Watch.Growth.afterTotalOf(Duration.standardSeconds(30)) : Watch.Growth.never()))
                .apply(Window.<MatchResult.Metadata>into(FixedWindows
                        .of(Duration.standardSeconds((Boolean.TRUE.equals(watchGrowthTerminationCondition)) ? 30 : 60)))
                        .withAllowedLateness(Duration.ZERO));
        PCollection<KV<String, String>> mycollection2 = collection1.apply(FileIO.readMatches()).apply(ParDo.of(new fileiojava8()));

    }
}
