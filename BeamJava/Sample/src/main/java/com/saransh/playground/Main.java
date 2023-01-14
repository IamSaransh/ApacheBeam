package com.saransh.playground;

import org.json.simple.parser.JSONParser;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.apache.beam.sdk.transforms.Watch;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;


public class Main {

    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();
        Boolean watchGrowthTerminationCondition = true;
        PCollection<MatchResult.Metadata> collection1 = pipeline
                .apply("Read Incoming New files",
                        FileIO.match().filepattern("C:\\Workspace\\ApacheBeam\\BeamJava\\src\\main\\resources\\Inputfiles\\ndjson\\ndjson.json")
                                .continuously(Duration.standardSeconds((watchGrowthTerminationCondition.equals(Boolean.TRUE)) ? 30 : 60),
                                        watchGrowthTerminationCondition.equals(Boolean.TRUE) ? Watch.Growth.afterTotalOf(Duration.standardSeconds(30)) : Watch.Growth.never()))
                .apply(Window.<MatchResult.Metadata>into(FixedWindows
                        .of(Duration.standardSeconds((Boolean.TRUE.equals(watchGrowthTerminationCondition)) ? 30 : 60)))
                        .withAllowedLateness(Duration.ZERO));
        PCollection<FileIO.ReadableFile> mycollection2 = collection1.apply(FileIO.readMatches());
//        PCollection<String> mycollection3 = mycollection2 .apply(TextIO.readFiles());
//        mycollection3.apply(ParDo.of(new printDoFn()));
        mycollection2.apply(new TextIOWithCustomLogic());
        pipeline.run().waitUntilFinish();
    }

//    private static class printDoFn extends DoFn<String, KV<String, String>> {
//        @DoFn.ProcessElement
//        public void processElement(ProcessContext c) throws ParseException {
//            System.out.println(c.element());
//            JSONParser jsonParser = new JSONParser();
//            JSONObject jsonObject = (JSONObject) jsonParser.parse(c.element());
//            if(jsonObject.containsKey("id")){
//                if( (Long)jsonObject.get("id")% 2==0){
//                    c.output(KV.of("even", c.element()));
//                    System.out.println(KV.of("even", c.element()));
//                }
//                if((Long)jsonObject.get("id")% 2!=0){
//                    c.output(KV.of("odd", c.element()));
//                    System.out.println(KV.of("odd", c.element()));
//                }
//            }
//        }
//    }
}
