package com.fantasy.clash.chat_service.utils;

import java.util.Calendar;
import java.util.TimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fantasy.clash.framework.utils.StringUtils;

public class TimeConversionUtils {
  private static final Logger logger = LoggerFactory.getLogger(TimeConversionUtils.class);

  public static Long getGMTTime() {
    long timeInMillis = Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTimeInMillis();
    return timeInMillis;
  }

  public static String convertTimeIntoString(Long timestamp) {
    try {
      String convertedTime = "";
      Integer time;
      Long second = 1000L;
      Long minute = 60000L;
      Long hour = 3600000L;
      Long day = 86400000L;
      // Long month = 2592000000L;
      if (timestamp < second) {
        convertedTime = "now";
      } else if (second <= timestamp && timestamp < minute) {
        time = (int) (timestamp / second);
        convertedTime = String.format("%s second(s) ago", time);
      } else if (minute <= timestamp && timestamp < hour) {
        time = (int) (timestamp / minute);
        convertedTime = String.format("%s minute(s) ago", time);
      } else if (hour <= timestamp && timestamp < day) {
        time = (int) (timestamp / hour);
        convertedTime = String.format("%s hour(s) ago", time);
      } else {
        time = (int) (timestamp / day);
        convertedTime = String.format("%s day(s) ago", time);
      }
      return convertedTime;
    } catch (Exception e) {
      logger.error(StringUtils.printStackTrace(e));
    }
    return null;
  }
}

