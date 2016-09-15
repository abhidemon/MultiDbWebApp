package main.java.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Config {
	
	private static Properties properties = null;
	static ObjectMapper objectMapper =  new ObjectMapper();
	
	public static void init(InputStream inputStream) throws IOException{
		Properties prop = new Properties();
	    prop.load(inputStream); // \n\t
        System.out.println(objectMapper.writeValueAsString(prop).replaceAll("\",","\", \n\t    ").replaceAll("\":\"", "\"     :     \""));
        properties = prop;
        System.out.println("Initialised Config. \n Initialising Log4j.");
        Log4jLogger.init();
	}

	public static Properties getProperties() {
		return properties;
	}

	public static String getProperty(String key){
		return properties.getProperty(key);
	}

	
}
