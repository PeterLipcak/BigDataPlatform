package densification;

import org.joda.time.DateTime;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class InterpolationDensificator implements IDensificator {

    private int densificationCount;
    private String[] interpolators;

    public InterpolationDensificator(int densificationCount, String[] interpolators){
        this.densificationCount = densificationCount;
        this.interpolators = interpolators;
    }

    @Override
    public List<String> densify(String record1, String record2) throws ParseException {
        List<String> recordsToIngest = new ArrayList<>();

        if(record1 != null && record2 == null){
            recordsToIngest.add(record1);
            return recordsToIngest;
        } else if (record1 == null) return recordsToIngest;

        String[] firstRecordSplits = record1.split(",");
        String[] secondRecordSplits = record2.split(",");

        String[][] recordsToIngestArray = new String[densificationCount][firstRecordSplits.length + 1];
        for(int i=0; i<recordsToIngestArray.length; i++){
            recordsToIngestArray[i] = firstRecordSplits.clone();
        }

        for(String interpolator : interpolators){
            String[] interpolatorSplits = interpolator.split(";");
            int position = Integer.parseInt(interpolatorSplits[0]);
            String type = interpolatorSplits[1].toLowerCase();

            switch (type){
                case "double" :
                    ingestValuesToArray(
                            recordsToIngestArray,
                            interpolate(Double.parseDouble(firstRecordSplits[position]),Double.parseDouble(secondRecordSplits[position]))
                            ,position);
                    break;
                case "integer" :
                    ingestValuesToArray(
                            recordsToIngestArray,
                            interpolate(Integer.parseInt(firstRecordSplits[position]),Integer.parseInt(secondRecordSplits[position]))
                            ,position);
                    break;
                case "long" :
                    ingestValuesToArray(
                            recordsToIngestArray,
                            interpolate(Long.parseLong(firstRecordSplits[position]),Long.parseLong(secondRecordSplits[position]))
                            ,position);
                    break;
                case "date" :
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(interpolatorSplits[2]);
                    ingestValuesToArray(
                            recordsToIngestArray,
                            interpolate(simpleDateFormat.parse(firstRecordSplits[position]),simpleDateFormat.parse(secondRecordSplits[position]), interpolatorSplits[2])
                            ,position);
                    break;
                    default :
                        break;

            }
        }

        for(String[] row : recordsToIngestArray){
            recordsToIngest.add(String.join(",",row));
        }

//        Double[] interpolatedValues = interpolate(Double.parseDouble(firstRecordSplits[2]),Double.parseDouble(secondRecordSplits[2]));
//        for(int i=0; i<densificationCount; i++){
//            recordsToIngest.add(firstRecordSplits[0]+ "," + firstRecordSplits[1] + "," + interpolatedValues[i]);
//        }

        return recordsToIngest;
    }

    private void ingestValuesToArray(String[][] recordsToIngestArray, String[] values, int position){
        for(int i=0; i<recordsToIngestArray.length; i++){
            recordsToIngestArray[i][position] = values[i];
        }
    }

    public String[] interpolate(Double start, Double end) {
        if (densificationCount < 2) {
            throw new IllegalArgumentException("interpolate: illegal count!");
        }
        String[] array = new String[densificationCount + 1];
        for (int i = 0; i <= densificationCount; ++ i) {
            array[i] = Double.toString(start + i * (end - start) / densificationCount);
        }
        return array;
    }

    public String[] interpolate(Integer start, Integer end) {
        if (densificationCount < 2) {
            throw new IllegalArgumentException("interpolate: illegal count!");
        }
        String[] array = new String[densificationCount + 1];
        for (int i = 0; i <= densificationCount; ++ i) {
            array[i] = Integer.toString(start + i * (end - start) / densificationCount);
        }
        return array;
    }

    public String[] interpolate(Long start, Long end) {
        if (densificationCount < 2) {
            throw new IllegalArgumentException("interpolate: illegal count!");
        }
        String[] array = new String[densificationCount + 1];
        for (int i = 0; i <= densificationCount; ++ i) {
            array[i] = Long.toString(start + i * (end - start) / densificationCount);
        }
        return array;
    }

    public String[] interpolate(Date start, Date end, String format) {
        if (densificationCount < 2) {
            throw new IllegalArgumentException("interpolate: illegal count!");
        }

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        String[] array = new String[densificationCount + 1];
        for (int i = 0; i <= densificationCount; ++ i) {
            array[i] = simpleDateFormat.format(new Date(start.getTime() + i * (end.getTime() - start.getTime()) / densificationCount));
        }

        return array;
    }


}
