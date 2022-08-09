package com.loggingevents.LoggingEvents.Consumer;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.BlockingQueue;

import com.loggingevents.LoggingEvents.Model.Event;

public class EventConsumer implements Runnable {


	  private BlockingQueue<Event> blockingQueue;
	  private Connection con;

	  public EventConsumer(BlockingQueue<Event> blockingQueue, Connection con) {
	    this.blockingQueue = blockingQueue;
	    this.con = con;
	  }

	  @Override
	  public void run() {
	    /**
	     * Consuming event one by one in a infinite loop.
	     * The Loop will break while there are no more
	     * event to consume
	     */
		  Statement stmt = null;
	      ResultSet result = null;
	    while (true) {
			
	      try {
	        Event eventObject = blockingQueue.take();
	        System.out.println("Consuming event :" + eventObject);
	        
	        stmt = con.createStatement();
	        result = stmt.executeQuery("SELECT * FROM EVENTS where event_id='"+eventObject.getEventId()+"'");
	        
	        if (result.next() == false) {
	        	System.out.println("ResultSet in empty for event_id: "+eventObject.getEventId());
	            stmt.executeUpdate("INSERT INTO  \"EVENTS\"  VALUES ('"+eventObject.getEventId()+"',"+eventObject.getEventDuration()+","
	            		+ "'"+eventObject.getEventType()+"','"+eventObject.getEventHost()+"','"+eventObject.getAlert()+"');"); 
	                 con.commit(); 
	                 System.out.println("New event added :" + eventObject.getEventId());
	          } else {
	            do {
	            	BigDecimal eventDurationFromDB = result.getBigDecimal("event_duration");
	            	BigDecimal eventDurationDiff;
	            	if("STARTED".equals(eventObject.getEventState())) {
	            		 eventDurationDiff = eventDurationFromDB.subtract(eventObject.getEventDuration());
	            	} else {
	            		eventDurationDiff = eventObject.getEventDuration().subtract(eventDurationFromDB);
	            	}
	            	char alert = eventDurationDiff.compareTo(BigDecimal.valueOf(4)) > 0 ? 'Y' : 'N' ;
	            	stmt.executeUpdate("UPDATE \"EVENTS\" SET alert = '"+alert+"',event_duration = "+eventDurationDiff+" WHERE event_id = '"+eventObject.getEventId()+"'"); 
	                     con.commit(); 
	                     System.out.println(" event updated :" + eventObject.getEventId());
	            } while (result.next());
	            }
	        
	      } catch (Exception e) {
	        e.printStackTrace();
	      } finally {
	      }
	    }
	  }

}
