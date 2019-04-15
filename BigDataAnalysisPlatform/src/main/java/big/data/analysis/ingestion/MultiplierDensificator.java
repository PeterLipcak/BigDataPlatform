package big.data.analysis.ingestion;

import java.util.ArrayList;
import java.util.List;


//NOT WORKING
public class MultiplierDensificator implements IDensificator {

    private int densificationCount = 2;

    public MultiplierDensificator(int densificationCount){
        this.densificationCount = densificationCount;
    }

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
