package com.loggingevents.LoggingEvents.Producer;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.concurrent.BlockingQueue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loggingevents.LoggingEvents.Model.Event;

public class EventProducer implements Runnable {

  private BlockingQueue<Event> blockingQueue;
  private String filePath;
  
  public EventProducer(BlockingQueue<Event> blockingQueue, String filePath) {
    this.blockingQueue = blockingQueue;
    this.filePath = filePath;
  }

  @Override
	public void run() {
		try {
			File file = new File(filePath);
			BufferedReader br = new BufferedReader(new FileReader(file));
			String eventString;
			ObjectMapper mapper = new ObjectMapper();
			while ((eventString = br.readLine()) != null) {
				Event eventObject = mapper.readValue(eventString, Event.class);
				System.out.println("New Event read from file:" + eventObject);
				blockingQueue.put(eventObject);
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
  
}