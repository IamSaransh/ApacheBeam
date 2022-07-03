package com.saransh.playground;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class TextIOWithCustomLogic extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<KV<String, String>>> {


    @Override
    public PCollection<KV<String, String>> expand(PCollection<FileIO.ReadableFile> input) {
        return input.apply(TextIO.readFiles())
            .apply(ParDo.of(new printDoFn()));
    }
    private static class printDoFn extends DoFn<String, KV<String, String>> {
        @DoFn.ProcessElement
        public void processElement(ProcessContext c) throws ParseException {
            System.out.println(c.element());
            JSONParser jsonParser = new JSONParser();
            JSONObject jsonObject = (JSONObject) jsonParser.parse(c.element());
            if(jsonObject.containsKey("id")){
                if( (Long)jsonObject.get("id")% 2==0){
                    c.output(KV.of("even", c.element()));
                    System.out.println(KV.of("even", c.element()));
                }
                if((Long)jsonObject.get("id")% 2!=0){
                    c.output(KV.of("odd", c.element()));
                    System.out.println(KV.of("odd", c.element()));
                }
            }
        }
    }
}

