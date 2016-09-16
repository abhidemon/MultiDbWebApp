package main.java.datasource.impl;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.bson.Document;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import main.java.datasource.dbUtils.MongoConn;
import main.java.datasource.interfaces.DataSource;
import main.java.util.Log4jLogger;

public class MongoDataSource implements DataSource {

	static ObjectMapper objectMapper = new ObjectMapper();
    private static String dbname = "db1";
    private static String collName = "employees";
    public static final String[] ignorableMongoDbColumns = {"_id"} ; 
    
    private static final String baseSelectQuery = "SELECT * FROM "+dbname+"."+collName+" where ";
    private static int maxRowsAllowed = 1000;
    private static Logger logger = Log4jLogger.getLogger(MongoDataSource.class);
	
	
	@Override
	public List<Map<String,Object>> getEveryData(Map<String, Object> fieldValueMap) throws Exception {
		logger.info("Fetching from mongodb.");
        MongoDatabase db = MongoConn.getMongoClient().getDatabase(dbname);
        Document condition =  new Document();
        
        for (String key : fieldValueMap.keySet()){
        	condition .put(key, fieldValueMap.get(key));
        }
        
        Document projection =  new Document();
        
        for (String ignoreableField : ignorableMongoDbColumns ){
        	projection.append(ignoreableField,0);
        }
        
        final List<Map<String,Object>> resultList = new LinkedList<Map<String,Object>>();
        MongoCursor<Document> resultItr = db.getCollection(collName).find(condition).projection(projection).iterator();
        Map<String,Object> mp = new LinkedHashMap<>();
        
        while(resultItr.hasNext()){
        	Document doc = resultItr.next();
        	mp.putAll(doc);
        	resultList.add(mp);
        }
        return resultList;
		
	}


	@Override
	public boolean createNewResource(Map<String, Object> fieldValuesForResource) throws Exception {
		
		logger.info("Fetching from mongodb.");
        MongoDatabase db = MongoConn.getMongoClient().getDatabase(dbname);
        Document entry =  new Document();
        
		for (String key : fieldValuesForResource.keySet()){
			entry.put(key, fieldValuesForResource.get(key));
		}
		
		try{
			db.getCollection(collName).insertOne(entry);
			return true;
		}catch(Exception e){
			logger.error(e);
			return false;
		}
	}
	

}
