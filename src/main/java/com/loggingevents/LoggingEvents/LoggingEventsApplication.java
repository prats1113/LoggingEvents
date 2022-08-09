package com.loggingevents.LoggingEvents;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import com.loggingevents.LoggingEvents.Consumer.EventConsumer;
import com.loggingevents.LoggingEvents.Model.Event;
import com.loggingevents.LoggingEvents.Producer.EventProducer;
public class LoggingEventsApplication {

	public static void main(String[] args) throws SQLException {
		
		Connection con = null;
		Statement stmt = null;
	      
	      try {
	         //Registering the HSQLDB JDBC driver
	         Class.forName("org.hsqldb.jdbc.JDBCDriver");
	         //Creating the connection with HSQLDB
	         con = DriverManager.getConnection("jdbc:hsqldb:hsql://localhost/testdb", "SA", "");
	         stmt = con.createStatement();
	         stmt.executeUpdate("CREATE TABLE events (event_id VARCHAR(50) NOT NULL, event_duration bigint ,event_type VARCHAR(20),event_host VARCHAR(50) , "
	         		+ "alert char(1) ,PRIMARY KEY (event_id)); ");
	         System.out.println("Table created successfully");
	      }  catch (Exception e) {
	         e.printStackTrace(System.out);
	      }
	      
	      BlockingQueue<Event> blockingQueue = new LinkedBlockingQueue<>();

			/**
			 * Create and START the EventProducer. The EventProducer will create events.
			 * Which will be served in 5 consumers
			 */
	      EventProducer eventProducer = new EventProducer(blockingQueue,"D:\\STS_work\\LoggingEvents\\src\\main\\resources\\test.txt");
			new Thread(eventProducer).start();

			EventConsumer eventConsumer = new EventConsumer(blockingQueue,con);
			ExecutorService executor = Executors.newFixedThreadPool(2);
			for (int i = 1; i <= 2; i++) {
				executor.submit(eventConsumer);
			}
			executor.shutdown();
	}

}
