package de.tu_berlin.dima.aim3.querysuggestion;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeUtils {

  /** querylog date time format */
  private static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  /**
   * Get date from string of sort yyyy-MM-dd HH:mm:ss
   * 
   * @param time
   * @return
   */
  static public Date parseTime(String time) {

    Date date = null;

    try {
      date = TIME_FORMAT.parse(time);
    } catch (ParseException e) {
      e.printStackTrace();
    }
    return date;
  }

  /**
   * Get date as long from string of sort yyyy-MM-dd HH:mm:ss
   * 
   * @param time
   * @return
   */
  static public long parseTimeToLong(String time) {

    return parseTime(time).getTime();
  }

  static public int timeDiffInSec(Long t1, Long t2) {

    // Get msec from each, and subtract.
    long diff = t2 - t1;
    long diffSeconds = diff / 1000;

    return (int) diffSeconds;
  }

  /**
   * calculate time difference in sec
   * 
   * @param d1
   * @param d2
   * @return
   */
  static public int timeDiffInSec(Date d1, Date d2) {

    // Get msec from each, and subtract.
    long diff = d2.getTime() - d1.getTime();
    long diffSeconds = diff / 1000;
    // long diffMinutes = diff / (60 * 1000);
    // long diffHours = diff / (60 * 60 * 1000);
    // System.out.println("Time in seconds: " + diffSeconds + " seconds.");
    // System.out.println("Time in minutes: " + diffMinutes + " minutes.");
    // System.out.println("Time in hours: " + diffHours + " hours.");

    return (int) diffSeconds;
  }

  /**
   * calculate time difference in sec
   * 
   * @param dateStart
   * @param dateStop
   * @return
   */
  static int timeDiffInSec(String dateStart, String dateStop) {

    Date d1 = parseTime(dateStart);
    Date d2 = parseTime(dateStop);

    return timeDiffInSec(d1, d2);
  }

  /**
   * calculate time difference in sec
   * 
   * @param d1
   * @param dateStop
   * @return
   */
  static public int timeDiffInSec(Date d1, String dateStop) {

    Date d2 = parseTime(dateStop);

    return timeDiffInSec(d1, d2);
  }

  /**
   * calculate time difference in sec
   * 
   * @param dateStart
   * @param d2
   * @return
   */
  static public int timeDiffInSec(String dateStart, Date d2) {

    Date d1 = parseTime(dateStart);

    return timeDiffInSec(d1, d2);
  }

}
