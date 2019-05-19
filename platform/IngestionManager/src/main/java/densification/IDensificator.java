package densification;

import java.text.ParseException;
import java.util.List;

/**
 * Generic densificator interface
 *
 * @author Peter Lipcak, Masaryk University
 */
public interface IDensificator {

    /**
     * Densifies the dataset - records are coming from right to left
     * @param record1 first record
     * @param record2 second record
     * @return densified records
     * @throws ParseException
     */
    public List<String> densify(String record1, String record2) throws ParseException;
}
