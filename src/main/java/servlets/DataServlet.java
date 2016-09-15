package main.java.servlets;

import main.java.datasource.factories.DataSourceFactory;
import main.java.datasource.interfaces.DataSource;
import main.java.util.Log4jLogger;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Created by abhishek.singh on 14/09/16.
 */

@WebServlet(name = "DataService",urlPatterns={"/hello"})
public class DataServlet extends HttpServlet {
	
	static Logger logger = Log4jLogger.getLogger(DataServlet.class);
	
	
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    	
		logger.info("Got request from "+request.getRemoteAddr());
		
        try {
            String dbSource = request.getParameter("dbSource");    
            if (dbSource==null){
            	logger.warn("received null dbSource .");
            	response.getWriter().append("Parameter dbSource cannot be left null");
            }
            String field = request.getParameter("field");
            if (field==null){
            	logger.warn("received null field .");
            	response.getWriter().append("Parameter field cannot be left null");
            }
            String value = request.getParameter("value");
            if (value==null){
            	logger.warn("received null value .");
            	response.getWriter().append("Parameter value cannot be left null");
            }
            String resp = "";
            try {
            	DataSource dataSource = DataSourceFactory.getDataSource(dbSource);
                resp = dataSource.getEveryDataForFieldAndValue(field, value);
            } catch (SQLException e) {
                resp = "Some Error Occured";
                e.printStackTrace();
            }
            response.getWriter().print(resp);
        }catch (Exception e ){
            response.getWriter().print(e);
            e.printStackTrace(response.getWriter());
        }

    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doPost(request,response);
    }
}