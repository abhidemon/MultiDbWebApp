package main.java.servlets;

import main.java.datasource.factories.DataSourceFactory;
import main.java.datasource.interfaces.DataSource;
import main.java.util.Log4jLogger;
import main.java.util.Tuple;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by abhishek.singh on 14/09/16.
 */

@WebServlet(name = "DataService", urlPatterns = { "/dbs/employees" })
public class DataServlet extends HttpServlet {

	static Logger logger = Log4jLogger.getLogger(DataServlet.class);
	static ObjectMapper objectMapper = new ObjectMapper();
	static final String[] compulsoryParamsForGet = { "dbSource" };
	static final String[] compulsoryParamsForPost = { "destDb" };

	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		logger.info("Got GET request from " + request.getRemoteAddr());
		
		try {
			
			Map<String, String[]> params = request.getParameterMap();
			
			String dbSource = request.getParameter("dbSource");
			
			String resp = "";
			try {
				Tuple<Boolean, Map<String, Object>> tup = verifyParams(params,compulsoryParamsForGet);
				if ( tup._1 ) {
					tup._2.remove("dbSource");
					DataSource dataSource = DataSourceFactory.getDataSource(dbSource);
					resp = objectMapper.writeValueAsString(dataSource.getEveryData(tup._2));
				}

			} catch (SQLException e) {
				resp = "Some Error Occured";
				e.printStackTrace();
			}
			response.getWriter().print(resp);
		} catch (Exception e) {
			response.getWriter().print(e);
			e.printStackTrace(response.getWriter());
		}

	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		logger.info("Got POST request from " + request.getRemoteAddr());
		Map<String, String[]> params = request.getParameterMap();
		Tuple<Boolean, Map<String, Object>> tup = verifyParams(params, compulsoryParamsForPost);
		if (tup._1) {
			try {
				Map<String, Object> fieldValueMap = tup._2;
				String destDb = params.get("destDb")[0];
				
				fieldValueMap.remove("destDb");
				if ( DataSourceFactory.getDataSource(destDb).createNewResource(fieldValueMap) ){
					response.getWriter().println( "Resource Created !!"  );
				}else{
					response.getWriter().println(" Could not create resource ");
				}
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error(e);
				
				e.printStackTrace();
			}

		}

	}

	private Tuple<Boolean,Map<String, Object>> verifyParams(Map<String, String[]> params, String[] compulsoryParams) {

		boolean isValid = true;
		Map<String, Object> mutableMapOfParams = new HashMap<>();
		for (String param : compulsoryParams) {
			if (!params.containsKey(param)) {
				isValid &= false;
				logger.warn("received null " + param);
			}
		}
		if (isValid){
			for (String key : params.keySet()){
				mutableMapOfParams.put(key, params.get(key)[0]);
			}
		}
				
		return new Tuple<Boolean, Map<String, Object>>(isValid,mutableMapOfParams);
	}

}