package com.saransh;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.Iterator;

public class SampleExcelInput {

    public static void main(String[] args) throws IOException {

        Pipeline pipeline = Pipeline.create();

        PCollection<FileIO.ReadableFile> inputCollection  = pipeline.apply(FileIO.match()
//                .filepattern("gs://bucket/file.xlsx"))
                        .filepattern("C:\\Users\\USER\\Desktop\\Java Masterclass\\ApacheBeam\\src\\main\\resources\\Inputfiles\\SampleExcel.xlsx"))
                .apply(FileIO.readMatches());

        PCollection<String> enrichedCollection = inputCollection.apply(ParDo.of(new ReadXlsxDoFn()));
        //TODO: do further processing treating the lines of enrichedCollection pcollection as if they were read from csv
        pipeline.run().waitUntilFinish();
    }

    static class ReadXlsxDoFn extends DoFn<FileIO.ReadableFile, String>{
        final static String  DELIMITER  = ";";
        @ProcessElement
        public void process(ProcessContext c) throws IOException {
            FileIO.ReadableFile  fileName = c.element();
            System.out.println("FileName being read is :" + fileName);
            assert fileName != null;
            InputStream stream = Channels.newInputStream(fileName.openSeekable());
            XSSFWorkbook wb = new XSSFWorkbook(stream);
            XSSFSheet sheet = wb.getSheetAt(0);     //creating a Sheet object to retrieve object
            //iterating over Excel file
            for (Row row : sheet) {
                Iterator<Cell> cellIterator = row.cellIterator();   //iterating over each column
                StringBuilder sb  = new StringBuilder();
                while (cellIterator.hasNext()) {
                    Cell cell = cellIterator.next();
                    if(cell.getCellType() ==  Cell.CELL_TYPE_NUMERIC){
                        sb.append(cell.getNumericCellValue()).append(DELIMITER);
                    }
                    else{
                        sb.append(cell.getStringCellValue()).append(DELIMITER);
                    }
                }
                System.out.println(sb.substring(0, sb.length()-1));
                c.output(sb.substring(0, sb.length()-1));//removing the delimiter present @End of String

            }
        }
    }
}
