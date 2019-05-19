package utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Helper methods for Time/Date manipulation
 * @author Peter Lipcak, Masaryk University
 */
public class TimeHelper {

    /**
     * Get date from date string
     * @param date as a string value
     * @return date based parsed from string
     * @throws ParseException when date string is incorrect
     */
    public static Date getDateFromString(String date) throws ParseException {
        String pattern = "yyyy-MM-dd HH:mm:ss";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        return simpleDateFormat.parse(date);
    }

}
