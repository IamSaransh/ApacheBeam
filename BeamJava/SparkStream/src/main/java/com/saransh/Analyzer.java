package com.saransh;

import com.saransh.SimpleFn.FormatAsTextFn;
import com.saransh.pardo.LevelExtracter;
import com.saransh.services.EmailService;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Map;

import static com.saransh.constantandconfig.Configurations.*;

public class Analyzer {
    private static final Logger log = LoggerFactory.getLogger(Analyzer.class);


    public static void main(String[] args) throws InterruptedException {

        DirectOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DirectOptions.class);
        options.setBlockOnRun(false);
        options.setRunner(DirectRunner.class);
        options.setTargetParallelism(100);


        Pipeline p = Pipeline.create(options);

        final PCollection<MatchResult.Metadata> metadataPCollection = p.apply(FileIO.match()
                        .filepattern(INPUT_LOG_QA_FOLDER)
                        .continuously(
                                Duration.standardSeconds(10),
                                Watch.Growth.<String>never()))
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(30))));


        final PCollection<KV<String, String>> kvOfLevelAndClassName = metadataPCollection.apply(FileIO.readMatches())
                .apply(TextIO.readFiles())
                .apply(ParDo.of(new LevelExtracter()));
        for (String level : LEVELS) {
            final PCollection<String> errorClasses = kvOfLevelAndClassName
                    .apply(Filter.by(x -> x.getKey().equals(level)))
                    .apply(Values.create());

            final PCollection<KV<String, Long>> countErrorPerClass = errorClasses.apply(Count.perElement());
            final PCollection<String> formattedCountErrorPerClass = countErrorPerClass.apply(MapElements.via(new FormatAsTextFn()));


            formattedCountErrorPerClass.apply(TextIO.write()
                            .to(OUTPUT_FILE_PATH + "-" + level + ".txt")
                            .withWindowedWrites()
                            .withoutSharding()

            );

        }


        p.run();

        log.info("Completed pipeline");
        int errrorCOunt = 0;
        while (errrorCOunt <= 10) {


            String htmlPageContent = readMyhtmlPage();
            for (String level : LEVELS) {
                StringBuilder childTableBody = new StringBuilder();
                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(Files.newInputStream(Paths.get(OUTPUT_FILE_PATH + "-" + level + ".txt")),
                                StandardCharsets.UTF_8))) {

                    String line;
                    while ((line = br.readLine()) != null) {
                        childTableBody.append(line);
                    }
                    if (childTableBody.toString().isEmpty()) {
                        childTableBody.append("<tr><td>NULL</td><td>NULL</td></td>");
                    }
                    htmlPageContent = htmlPageContent.replace("REPLACE_" + level, childTableBody.toString());
                } catch (Exception e) {
                    log.error("Error Occurred in Reading the File ");
                    log.error("The error was {}", e.getLocalizedMessage());
                    errrorCOunt++;
                }
            }


        }
    }


    private static String readMyhtmlPage() {
        StringBuilder htmlBody = new StringBuilder();

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(Files.newInputStream(Paths.get("src\\main\\resources\\index.html")),
                        StandardCharsets.UTF_8))) {

            String line;
            while ((line = br.readLine()) != null) {
                htmlBody.append(line);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return htmlBody.toString();
    }
}

