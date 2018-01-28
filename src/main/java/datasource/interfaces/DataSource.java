package main.java.datasource.interfaces;

import java.util.List;
import java.util.Map;

public interface DataSource {
	
	public List<Map<String,Object>> getEveryData(Map<String, Object> fieldValueMap) throws Exception;

	public boolean createNewResource(Map<String, Object> fieldValueMap) throws Exception;


	
}

