package mongoutil;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

import java.util.Arrays;
import java.util.HashMap;

/**
 * Created by abhishek.singh on 14/09/16.
 */
public class MongoConn {

    static MongoClient mongoClient=null;
    static HashMap<String,MongoDatabase> dbHandlerTable =  new HashMap<String, MongoDatabase>();

    private static synchronized void init(){
        if (mongoClient==null){
            mongoClient = new MongoClient(
                    Arrays.asList(new ServerAddress("localhost", 27017),
                            new ServerAddress("localhost", 27018),
                            new ServerAddress("localhost", 27019)));

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
