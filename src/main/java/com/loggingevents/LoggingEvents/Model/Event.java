package com.loggingevents.LoggingEvents.Model;
import java.math.BigDecimal;

import lombok.Data;

@Data
public class Event {

  private String eventId;
  private BigDecimal eventDuration;
  private String eventState;
  private String eventType;
  private String eventHost;
  private char alert;
  
}