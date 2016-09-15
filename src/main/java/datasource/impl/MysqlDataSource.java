package main.java.datasource.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.internal.SessionImpl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import main.java.datasource.dbUtils.HibernateUtil;
import main.java.datasource.interfaces.DataSource;
import main.java.util.Log4jLogger;

public class MysqlDataSource implements DataSource{
	
	
	static ObjectMapper objectMapper = new ObjectMapper();
    private static String dbname = "db1";
    private static String collName = "employees";
    private static final String[] fieldsInMysqlTable = {"name","age"};

    private static final String baseSelectQuery = "SELECT * FROM "+dbname+"."+collName+" where ";
    
    private static int maxRowsAllowed = 1000;
    private static Logger logger = Log4jLogger.getLogger(MongoDataSource.class);
	
	
	@Override
	public String getEveryDataForFieldAndValue(String field, String value) throws SQLException, JsonProcessingException {
		// 
		logger.info("Retrieving data from mysql");
		String fetchString = baseSelectQuery+" "+new String(field)+"= ? ";		
		
		Session hibernateSession = HibernateUtil.getSessionFactory().openSession();
        Connection conn = ((SessionImpl) hibernateSession).connection();
        PreparedStatement preparedStatement = conn.prepareStatement(fetchString);
        preparedStatement.setString(1, value);
        logger.info("Executing Mysql Query : "+fetchString);
    	java.sql.ResultSet rs = preparedStatement.executeQuery();
        List<Map<String,Object>> listOfEntries = new LinkedList<>();
        while(rs.next()){
            Map<String,Object> entry = new HashMap<>();
            for (String col : fieldsInMysqlTable){
                entry.put(col,rs.getObject(col));
            }
            listOfEntries.add(entry);
        }
        logger.info("Retrived results from mysql");
        return objectMapper.writeValueAsString(listOfEntries);

	}
	
}
