package com.saransh.playground;


import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.io.IOUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 *  DOFN that reads the matched file In memory
 *  --> splits it into lines
 *  --> creates KV of Success & failure Messages
 */
public class fileiojava8 extends DoFn<FileIO.ReadableFile, KV<String, String>> {
    private static final Logger LOG = LoggerFactory.getLogger(fileiojava8.class);
    public static final String DQ_VALID_RECORD_IND = "dq_valid_record_ind";
    public static final String WARNING = "Warning";
    public static final String PASS = "Pass";
    public static final String ERROR = "Error";
    public static final String EMPTY_FILE_KV = "Empty-File-kv";

    @DoFn.ProcessElement
    public void process(ProcessContext c) throws IOException, ParseException {
            FileIO.ReadableFile f = c.element();
            assert f != null;
            String filename = f.getMetadata().resourceId().toString();
            LOG.info("File being Read is:{}", filename);

            //read file into stream, try-with-resources
            try (Stream<String> stream = Files.lines(Paths.get(filename)))
        {
                List<String> doc = new ArrayList<>();
                stream.forEach(doc::add);
            if(doc.isEmpty())
                c.output(KV.of(EMPTY_FILE_KV,""));
            else{
                JSONParser jsonParser = new JSONParser();
                for (String NDJOSNString : doc) {
                    JSONObject jsonObject = (JSONObject) jsonParser.parse(NDJOSNString);
                    if(jsonObject.containsKey("somthing")) {
                        c.output(KV.of("demo", NDJOSNString));
                    }
                }
            }
        }
            catch (Exception e){
                e.printStackTrace();
            }

    }
}