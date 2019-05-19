package densification;

import java.text.ParseException;
import java.util.List;

public interface IDensificator {
    public List<String> densify(String record1, String record2) throws ParseException;
}
