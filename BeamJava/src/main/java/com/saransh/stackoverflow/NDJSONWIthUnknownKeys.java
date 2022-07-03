package com.saransh.stackoverflow;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;

public class NDJSONWIthUnknownKeys {

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<FileIO.ReadableFile> p1 = pipeline.apply(FileIO.match()
                .filepattern("C:\\Workspace\\ApacheBeam\\BeamJava\\src\\main\\resources\\Inputfiles\\ndjson\\ndjson.json"))
                .apply(FileIO.readMatches());
        PCollectionView<String> csvHeaderView = p1.apply
                (ParDo.of(new HeaderExtractDoFn()))
                .apply(View.asSingleton());
        /* TODO: use this(csvHeaderView) as side input to the Text.IO.write as header but its not supported right now
        Links:
        1.https://stackoverflow.com/questions/45649244/google-cloud-dataflow-apache-beam-can-i-use-sideinputs-with-textio-write
        2.https://github.com/apache/beam/issues/18464 (Latest Comment on June4,2022)
        */

        PCollection<String> header = p1.apply(ParDo.of( new HeaderExtractDoFn()));
        PCollection<String> jsonFileValues = p1.apply(TextIO.readFiles())
                .apply(ParDo.of( new ValueExtractDoFn()));;
        PCollection<String> headerAndValues = PCollectionList.of(header).and(jsonFileValues).
                apply(Flatten.<String>pCollections());

        headerAndValues.apply("Write just the header",
                        TextIO.write()
                                .withNumShards(1).withSuffix(".csv")
                                .to("C:\\Workspace\\ApacheBeam\\BeamJava\\src\\main\\resources\\Inputfiles\\ndjson\\OP.json"));

        pipeline.run().waitUntilFinish();
        System.out.println("Done with pipeline");
    }
    static class HeaderExtractDoFn extends DoFn<FileIO.ReadableFile, String>{
        final static String  DELIMITER  = ";";
        @ProcessElement
        public void process(ProcessContext c) throws IOException, ParseException {
            FileIO.ReadableFile fileName = c.element();
            FileReader file = new FileReader( fileName.getMetadata().resourceId().toString());
            BufferedReader buffer = new BufferedReader(file);
            //read the 1st line
            String line = buffer.readLine();
            JSONParser parser = new JSONParser();
            JSONObject inputLine = (JSONObject) parser.parse(line);
            StringBuilder sb = new StringBuilder();
            for(Object key: inputLine.keySet())
                sb.append(key.toString()).append(DELIMITER);
            c.output(sb.toString().substring(0, sb.length()-1));
        }
    }

    static class ValueExtractDoFn extends DoFn<String, String> {
        final static String  DELIMITER  = ";";
        @DoFn.ProcessElement
        public void processLines(@Element String line, OutputReceiver<String> receiver)
                throws ParseException {
            JSONParser parser = new JSONParser();
            JSONObject inputLine = (JSONObject) parser.parse(line);
            StringBuilder sb = new StringBuilder();
            for(Object value: inputLine.values())
                sb.append(value.toString()).append(DELIMITER);
            receiver.output(sb.toString().substring(0, sb.length()-1));
        }
    }


}
