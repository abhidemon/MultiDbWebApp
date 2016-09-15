package main.java.datasource.impl;

import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.bson.Document;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import main.java.datasource.interfaces.DataSource;
import main.java.util.Log4jLogger;
import main.java.util.mongoutil.MongoConn;

public class MongoDataSource implements DataSource {

	static ObjectMapper objectMapper = new ObjectMapper();
    private static String dbname = "db1";
    private static String collName = "employees";
    public static final String[] ignorableMongoDbColumns = {"_id"} ; 
    
    private static final String baseSelectQuery = "SELECT * FROM "+dbname+"."+collName+" where ";
    private static int maxRowsAllowed = 1000;
    private static Logger logger = Log4jLogger.getLogger(MongoDataSource.class);
	
	
	@Override
	public String getEveryDataForFieldAndValue(String field, String value) throws Exception {
		logger.info("Fetching from mongodb.");
        MongoDatabase db = MongoConn.getMongoClient().getDatabase(dbname);
        Document condition =  new Document(field,value);
        Document projection =  new Document();
        
        for (String ignoreableField : ignorableMongoDbColumns ){
        	projection.append(ignoreableField,0);
        }
        final List<Document> resultList = new LinkedList<Document>();
        MongoCursor<Document> resultItr = db.getCollection(collName).find(condition).projection(projection).iterator();
        
        while(resultItr.hasNext()){
        	Document doc = resultItr.next();
        	resultList.add(doc);
        }
        return objectMapper.writeValueAsString(resultList);
		
	}

}
