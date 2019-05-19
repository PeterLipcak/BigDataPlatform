package big.data.analysis.ingestion;

import java.util.ArrayList;
import java.util.List;


//NOT WORKING
public class InterpolationDensificator implements IDensificator {

    private int densificationCount = 2;

    public InterpolationDensificator(int densificationCount){
        this.densificationCount = densificationCount;
    }

    @Override
    public List<String> densify(String record1, String record2) {
        List<String> recordsToIngest = new ArrayList<>();
        if(record1 != null && record2 == null){
            recordsToIngest.add(record1);
            return recordsToIngest;
        }else if (record1 == null) return recordsToIngest;

        String[] firstRecordSplits = record1.split(",");
        String[] secondRecordSplits = record2.split(",");

        double[] interpolatedValues = interpolate(Double.parseDouble(firstRecordSplits[2]),Double.parseDouble(secondRecordSplits[2]), densificationCount);
        for(int i=0; i<densificationCount; i++){
            recordsToIngest.add(firstRecordSplits[0]+ "," + firstRecordSplits[1] + "," + interpolatedValues[i]);
        }

        return recordsToIngest;
    }

    public static double[] interpolate(double start, double end, int count) {
        if (count < 2) {
            throw new IllegalArgumentException("interpolate: illegal count!");
        }
        double[] array = new double[count + 1];
        for (int i = 0; i <= count; ++ i) {
            array[i] = start + i * (end - start) / count;
        }
        return array;
    }

}
