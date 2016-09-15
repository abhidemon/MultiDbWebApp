package main.java.mongoutil;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

import main.java.util.Config;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by abhishek.singh on 14/09/16.
 */
public class MongoConn {

    static MongoClient mongoClient=null;
    static HashMap<String,MongoDatabase> dbHandlerTable =  new HashMap<String, MongoDatabase>();

    private static synchronized void init(){
        if (mongoClient==null){
        	String serverList = Config.getProperty("mongo_server_list");
            if (serverList.contains("\\s")){
            	serverList=serverList.replaceAll("\\s","");
                
            }
        	String[] serverNamesList = serverList.split(",");
            List <ServerAddress> serverAddressList = new LinkedList<>();
            for (String serverName : serverNamesList){
                String host = serverName.substring(0,serverName.indexOf(":"));
                Integer port = Integer.parseInt(serverName.substring(serverName.indexOf(":")+1,serverName.length()));
                serverAddressList.add(new ServerAddress(host,port));
            }
            mongoClient = new MongoClient(serverAddressList);
        }

    }

    public static MongoClient getMongoClient(){
        if (mongoClient==null){
            init();
        }
        return mongoClient;
    }

    public static MongoDatabase getMongoDbHandle(String dbname){
        if (dbHandlerTable.containsKey(dbname)){
            return dbHandlerTable.get(dbname);
        }
        MongoDatabase databaseHandle = mongoClient.getDatabase(dbname);
        dbHandlerTable.put(dbname,databaseHandle);
        return databaseHandle;
    }



}