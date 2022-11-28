package com.fantasy.clash.chat_service.utils;

import java.util.Calendar;
import java.util.TimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeConversionUtils {
  private static final Logger logger = LoggerFactory.getLogger(TimeConversionUtils.class);
  public static Long getGMTTime() {
    long timeInMillis = Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTimeInMillis();
    logger.info("timeInMillis {}", timeInMillis);
    return timeInMillis;
  }

}
