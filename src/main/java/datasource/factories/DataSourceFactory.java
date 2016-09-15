package main.java.datasource.factories;

import main.java.datasource.impl.MongoDataSource;
import main.java.datasource.impl.MysqlDataSource;
import main.java.datasource.interfaces.DataSource;

public class DataSourceFactory {
	
	public static DataSource getDataSource(String sourceName) throws Exception  {
        switch (sourceName.toLowerCase()){
	        case "mongodb": return new MongoDataSource();
	
	        case "mysql"  : return new MysqlDataSource();
	
	        default: throw new Exception("Unknown dbSource : "+sourceName+" has been selected. Currently only 'mongodb' and 'mysql'  are supported.");
        }
	}
	
}
