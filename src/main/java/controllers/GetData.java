package main.java.controllers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import main.java.mongoutil.MongoConn;
import main.java.mysqlutil.HibernateUtil;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.hibernate.Session;
import org.hibernate.internal.SessionImpl;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by abhishek.singh on 14/09/16.
 */
public class GetData {


    static ObjectMapper objectMapper = new ObjectMapper();
    private static String dbname = "db1";
    private static String collName = "employees";
    public static final String[] ignorableMongoDbColumns = {"_id"} ; 
    
    private static final String baseSelectQuery = "SELECT * FROM "+dbname+"."+collName+" where ";
    private static final String[] fieldsInMysqlTable = {"name","age"};
    private static int maxRowsAllowed = 1000;
    


    public static String getData(String dbSource,String field,String value) throws JsonProcessingException, SQLException {
        dbSource = dbSource.toLowerCase();
        switch (dbSource){
            case "mongodb": return getDataFromMongoDb(field,value);

            case "mysql"  : return getDataFromMySql(field,value);

            default: return "Unknown dbSource : "+dbSource+" has been selected. Currently only 'mongodb' and 'mysql'  are supported.";
        }

    }

    public static String getDataFromMongoDb(String field,String value) throws JsonProcessingException {

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

    public static String getDataFromMySql(String field,String value) throws SQLException, JsonProcessingException {
        Session hibernateSession = HibernateUtil.getSessionFactory().openSession();
        Connection conn = ((SessionImpl) hibernateSession).connection();
        java.sql.Statement statement = conn.createStatement();
        String fetchString = baseSelectQuery+" "+new String(field)+"='"+new String(value)+"'";
        System.out.println(fetchString);
        java.sql.ResultSet rs = statement.executeQuery(fetchString);
        List<Map<String,Object>> listOfEntries = new LinkedList();
        while(rs.next()){
            Map<String,Object> entry = new HashMap<>();
            for (String col : fieldsInMysqlTable){
                entry.put(col,rs.getObject(col));
            }
            listOfEntries.add(entry);
        }
        return objectMapper.writeValueAsString(listOfEntries);
    }

}
