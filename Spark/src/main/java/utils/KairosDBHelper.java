package utils;

import entities.Consumption;
import org.kairosdb.client.builder.DataFormatException;
import org.kairosdb.client.builder.DataPoint;
import org.kairosdb.client.response.Query;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.Result;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class KairosDBHelper {

    public static List<DataPoint> getDataPointsFromResponse(QueryResponse response, String metricTable) throws IOException {
        for (Query query : response.getQueries()) {
            for (Result result : query.getResults()) {
                if (result.getName().compareTo(metricTable) == 0) {
                    return result.getDataPoints();
                }
            }
        }

        return new ArrayList<>();
    }

    public static List<Consumption> getConsumptionsFromResponse(QueryResponse response, String metricTable, int id) throws IOException, DataFormatException {
        for (Query query : response.getQueries()) {
            for (Result result : query.getResults()) {
                if (result.getName().compareTo(metricTable) == 0) {
                    List<Consumption> consumptions = new ArrayList<>();
                    for(DataPoint dataPoint : result.getDataPoints()){
                        consumptions.add(new Consumption(id,new Date(dataPoint.getTimestamp()),dataPoint.doubleValue()));
                    }
                    return consumptions;
                }
            }
        }

        return new ArrayList<>();
    }

}
