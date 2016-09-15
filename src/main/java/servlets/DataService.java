package main.java.servlets;

import main.java.controllers.GetData;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.SQLException;

/**
 * Created by abhishek.singh on 14/09/16.
 */
@WebServlet(name = "DataService",urlPatterns={"/hello"})
public class DataService extends HttpServlet {
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    	

        try {
            String dbSource = request.getParameter("dbSource");
            
            String field = request.getParameter("field");
            String value = request.getParameter("value");
            String resp = "";
            try {
                resp = GetData.getData(dbSource, field, value);
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
