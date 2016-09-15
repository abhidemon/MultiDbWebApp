package main.java.util;


import org.apache.log4j.*;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class Log4jLogger {

    private Logger log;
    private static Logger rootLogger;

    public Logger getLogger() {
        return log;
    }

    public static void init(){
        rootLogger = Logger.getRootLogger();
        Properties prop = null ;
        switch (prop.getProperty("logLevel","INFO").toUpperCase()){
            case "DEBUG" : rootLogger.setLevel(Level.DEBUG);
                break;
            case "INFO" : rootLogger.setLevel(Level.INFO);
                break;
            case "WARN" : rootLogger.setLevel(Level.WARN);
                break;
            case "ERROR" : rootLogger.setLevel(Level.ERROR);
                break;
            case "FATAL" : rootLogger.setLevel(Level.FATAL);
                break;
            default:
                rootLogger.setLevel(Level.ALL);
        }


        org.apache.log4j.PatternLayout layout = new org.apache.log4j.PatternLayout(prop.getProperty("patternLayout","%d{ISO8601} [%t] %-5p %x - %m%n")
        );

        try {
            DailyRollingFileAppender drfAppender = new DailyRollingFileAppender();
            String file = prop.getProperty("logFilePath","/var/log/aggregateLogs");
            drfAppender.setFile(file);
            drfAppender.setDatePattern("'.'yyyy-MM-dd-HH");
            drfAppender.setLayout(layout);
            drfAppender.activateOptions();
            rootLogger.addAppender(drfAppender);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Failed to add file appender , logging to console. !!");
        }
    }

    public Log4jLogger(Class className) throws ClassNotFoundException, IOException {
        log = Logger.getLogger(className);
    }


}
