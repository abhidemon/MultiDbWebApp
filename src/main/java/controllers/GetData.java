package controllers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import mongoutil.MongoConn;
import mysqlutil.HibernateUtil;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.hibernate.Hibernate;
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
    private static String dbname = "test";
    private static String collName = "abc";

    private static final String baseSelectQuery = "SELECT * FROM db1.tab1 where ";
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
        Bson condition =  new Document(field,value);
        final List<Document> resultList = new LinkedList<Document>();
        FindIterable<Document> resultItr = db.getCollection(collName).find(condition);
        resultItr.forEach(new Block<Document>() {

            public void apply(final Document document) {
                resultList.add(document);
                System.out.println(document);

            }
        });
        return objectMapper.writeValueAsString(resultList);
    }

    public static String getDataFromMySql(String field,String value) throws SQLException, JsonProcessingException {
        Session hibernateSession = HibernateUtil.getSessionFactory().openSession();
        Connection conn = ((SessionImpl) hibernateSession).connection();
        java.sql.Statement statement = conn.createStatement();
        String fetchString = baseSelectQuery+" "+new String(field)+"="+new String(value);
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
