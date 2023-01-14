package com.saransh.constantandconfig;

import java.util.Arrays;
import java.util.List;

public class Configurations {

    public static final String INPUT_LOG_QA_FOLDER = "E:\\logs\\*";
    public static final String  OUTPUT_FILE_PATH = "E:\\stats";
    public static final String CC_LIST = "saranshvashistha@gmail.com";
    public static final String TO_LIST = "saranshvashistha@gmail.com";
    public static final int POLL_TIME_MINS = 60;
    public static final int POLL_WINDOW_HOURS = 3;
    public static final int POLL_WINDOW_HOURS_MS = POLL_WINDOW_HOURS*60*60*1000;

    public static final List<String> LEVELS = Arrays.asList("INFO", "ERROR", "WARN");


}
