package main.java.datasource.dbUtils;

import java.util.Properties;

import org.apache.log4j.Logger;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.internal.SessionImpl;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;

import main.java.servlets.InitServlet;
import main.java.util.Config;
import main.java.util.Log4jLogger;

/**
 * Created by abhishek.singh on 12/22/15.
 */

public class HibernateUtil {

    private static SessionFactory sessionFactory;
    private static ServiceRegistry serviceRegistry;
    static int mysqlValidityTimeoutInSec=10;
    static Logger logger = Log4jLogger.getLogger(HibernateUtil.class);
    
    public static synchronized void init(){
    	if (sessionFactory!=null){
    		return;
    	}
    	logger.info("Initialising Mysql for the first time.");
        try {
        	Properties properties = Config.getProperties();
            String host             = properties.getProperty("mysqlHost");
            String dbName           = properties.getProperty("mysqlDbName");;
            String username         = properties.getProperty("mysqlUsername");
            //String password         = properties.getProperty("mysqlPassword");
            String mysqlPoolSize       = "50";

            Configuration conf = new Configuration();
            conf.setProperty("hibernate.connection.driver_class", "com.mysql.jdbc.Driver");
            conf.setProperty("hibernate.connection.username", username);
            //conf.setProperty("hibernate.connection.password", password);
            conf.setProperty("hibernate.connection.url", "jdbc:mysql://" + host + ":3306/" + dbName);
            conf.setProperty("hibernate.dialect", "org.hibernate.dialect.MySQLInnoDBDialect");
            conf.setProperty("hibernate.current_session_context_class", "thread");
            conf.setProperty("show_sql", "true");
            conf.setProperty("connection.pool_size", mysqlPoolSize);
            conf.setProperty("connection.provider_class", "org.hibernate.connection.C3P0ConnectionProvider");
            /*conf.setProperty("hibernate.c3p0.acquire_increment", 1+"");
            conf.setProperty("hibernate.c3p0.idle_test_period", 150+"");
            conf.setProperty("hibernate.c3p0.max_size", 100+"");
            conf.setProperty("hibernate.c3p0.max_statements", 0+"");
            conf.setProperty("hibernate.c3p0.min_size", 10+"");
            conf.setProperty("hibernate.c3p0.timeout", 100+"");
            conf.setProperty("hibernate.c3p0.idleConnectionTestPeriod", 5+"");
            conf.setProperty("hibernate.c3p0.testConnectionOnCheckout", true+"");
            */conf.setProperty("c3p0.acquire_increment", 1+"");
            conf.setProperty("c3p0.idle_test_period", 150+"");
            conf.setProperty("c3p0.max_size", 100+"");
            conf.setProperty("c3p0.max_statements", 0+"");
            conf.setProperty("c3p0.min_size", 10+"");
            conf.setProperty("c3p0.timeout", 100+"");
            conf.setProperty("c3p0.testConnectionOnCheckout", true+"");
            serviceRegistry = new ServiceRegistryBuilder().applySettings(conf.getProperties()).buildServiceRegistry();
            sessionFactory = conf.buildSessionFactory(serviceRegistry);
            mysqlValidityTimeoutInSec = 120;
        } catch (Exception e) {
        	logger.error("Some error occured  while initialising Mysql.", e);
            throw new ExceptionInInitializerError(e);
        }
        logger.info("Initialised Mysql successfully.");
        
    }

    public static SessionFactory getSessionFactory() {
        
        return sessionFactory;
    }


}