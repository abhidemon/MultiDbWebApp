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
    private static final Map<String,String> fieldsAndTypesInMysqlTable = new HashMap<>();
    static{
    	fieldsAndTypesInMysqlTable.put("name",String.class.getName() );
    	fieldsAndTypesInMysqlTable.put("age",Integer.class.getName() );
    	fieldsAndTypesInMysqlTable.put("score",Integer.class.getName() );
    	
    }
    
    private static final String baseSelectQuery = "SELECT * FROM "+dbname+"."+collName+" where ";
    private static final String baseInsertQuery = "Insert into "+dbname+"."+collName+" ";
    
    private static int maxRowsAllowed = 1000;
    private static Logger logger = Log4jLogger.getLogger(MongoDataSource.class);
	
	
	@Override
	public List<Map<String,Object>> getEveryData(Map<String, Object> fieldValueMap) throws SQLException, JsonProcessingException {
		// 
		logger.info("Retrieving data from mysql");
		String fetchString = baseSelectQuery+" ";
		
		int cnt = 0;
		for (String key : fieldValueMap.keySet()){
			if (cnt++ > 0){
				fetchString+=" and  ";
			}
			fetchString+= "  "+key+"=?";
		}
		
		Session hibernateSession = HibernateUtil.getSessionFactory().openSession();
        Connection conn = ((SessionImpl) hibernateSession).connection();
        PreparedStatement preparedStatement = conn.prepareStatement(fetchString);
        
        int cntV = 0;
		for (String key : fieldValueMap.keySet()){
			cntV ++;
			preparedStatement.setObject(cntV, fieldValueMap.get(key) );
		}
		
        logger.info("Executing Mysql Query : "+fetchString);
    	java.sql.ResultSet rs = preparedStatement.executeQuery();
        List<Map<String,Object>> listOfEntries = new LinkedList<>();
        while(rs.next()){
            Map<String,Object> entry = new HashMap<>();
            for (String col : fieldsAndTypesInMysqlTable.keySet()){
                entry.put(col,rs.getObject(col));
            }
            listOfEntries.add(entry);
        }
        logger.info("Retrived results from mysql");
        return listOfEntries;

	}


	@Override
	public boolean createNewResource(Map<String, Object> fieldValueMap) throws Exception {
		// TODO Auto-generated method stub
		Connection conn=null;
		try{
			
		logger.info("Inserting  data into mysql");
		String columns = " ( ";
		String values = " ( ";
		
		int cnt = 0;
		for (String key : fieldValueMap.keySet()){
			if (cnt++ > 0){
				columns += " , ";
				values  += " , ";
			}
			columns += " "+key+" ";
			values  +=  " ? ";
		}
		columns += " ) ";
		values  += " ) ";
		
		String insertQuery = baseInsertQuery + columns + " values "+ values +" ; ";
		Session hibernateSession = HibernateUtil.getSessionFactory().openSession();
        conn = ((SessionImpl) hibernateSession).connection();
        PreparedStatement preparedStatement = conn.prepareStatement(insertQuery );
        
        int cntV = 0;
		for (String key : fieldValueMap.keySet()){
			cntV ++;
			setValueWithType(preparedStatement,cntV, fieldValueMap.get(key), fieldsAndTypesInMysqlTable.get(key) );
		}
		
        logger.info("Executing Mysql Query : "+ insertQuery );
    	int count = preparedStatement.executeUpdate();
    	if (count>0){
    		return true;
    	}
    	
    	}catch(Exception e){
			logger.error(e);
			e.printStackTrace();
			return false;
		}finally{
			try{
				conn.close();
			}catch(Exception e){
				logger.error(e);
				e.printStackTrace();
			}
		}
		return false;
	}
	
	public static void setValueWithType(PreparedStatement ps,int index,Object value,String className) throws Exception{
		switch(className){
		case "java.lang.String": ps.setString(index, (String)value);
						break;
		case "java.lang.Integer" : ps.setInt(index, Integer.parseInt( (String)value) );
						break;
		default: throw new Exception("Unconfiggured Type "+className);
		}
	}
	
}
