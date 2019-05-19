package densification;

import java.util.ArrayList;
import java.util.List;


/**
 * Implementation of multiplier densificator
 *
 * @author Peter Lipcak, Masaryk University
 */
public class MultiplierDensificator implements IDensificator {

    private int densificationCount;

    public MultiplierDensificator(int densificationCount){
        this.densificationCount = densificationCount;
    }

    /**
     * Multiply each records by the amount specified in densification count
     * @param record1 first record
     * @param record2 second record
     * @return multiplied records
     */
    @Override
    public List<String> densify(String record1, String record2) {
        List<String> recordsToIngest = new ArrayList<>();
        if(record2 == null) return recordsToIngest;
        for(int i=0; i<densificationCount; i++){
            recordsToIngest.add(record2);
        }

        return recordsToIngest;
    }
}
