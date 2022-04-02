package com.saransh;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.time.Instant;


public class FileIO {
    public static  void main(String[] args){
        Pipeline p = Pipeline.create();

        PCollection<String> fileCollection = p
                .apply(TextIO.read().from("C:\\Users\\USER\\Desktop\\Java Masterclass\\ApacheBeam\\src\\main\\resources\\Inputfiles\\TextIO.csv"));

         fileCollection.apply(ParDo.of(new printDoFn()))
                 .apply(TextIO.write().to("C:\\Users\\USER\\Desktop\\Java Masterclass\\ApacheBeam\\src\\main\\resources\\outputfiles\\fileIo.csv")
                .withNumShards(2).withSuffix(".csv"));

        p.run().waitUntilFinish();
    }
    private static class printDoFn extends DoFn<String, String>{
        @DoFn.ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println(c.element());
            c.output(c.element());
        }

    }

}
