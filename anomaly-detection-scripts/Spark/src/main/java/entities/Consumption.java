package entities;

import org.joda.time.DateTime;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

/**
 * @author Peter Lipcak, Masaryk University
 */
public class Consumption implements Serializable {
    private int id;
    private Date measurementTimestamp;
    private double consumption;

    public Consumption(String csvRow) throws ParseException {
        String[] values = csvRow.split(",");
        String pattern = "yyyy-MM-dd HH:mm:ss";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        id = Integer.parseInt(values[0]);
        measurementTimestamp = simpleDateFormat.parse(values[1]);
        consumption = Double.parseDouble(values[2]);
    }

    public Consumption(int id, Date measurementTimestamp, double consumption) {
        this.id = id;
        this.measurementTimestamp = measurementTimestamp;
        this.consumption = consumption;
    }

    public Date getMeasurementTimestamp() {
        return measurementTimestamp;
    }

    public void setMeasurementTimestamp(Date measurementTimestamp) {
        this.measurementTimestamp = measurementTimestamp;
    }

    public double getConsumption() {
        return consumption;
    }

    public void setConsumption(int consumption) {
        this.consumption = consumption;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCompositeId(){
        DateTime consumptionDate = new DateTime(measurementTimestamp);
        String compositeId = id + "-" + consumptionDate.getYear() + "-" + consumptionDate.getDayOfYear() + "-" + consumptionDate.getHourOfDay();
        return compositeId;
    }

    @Override
    public boolean equals(Object other){
        if (this == other) return true;
        if (!(other instanceof Consumption)) return false;
        final Consumption that = (Consumption) other;
        return this.getId() == that.getId()
                && this.getMeasurementTimestamp().equals(((Consumption) other).getMeasurementTimestamp())
                && this.getConsumption() == ((Consumption) other).getConsumption();
    }

    @Override
    public int hashCode(){
        int prime = 31;
        int result = 1;
        result = prime * result + Integer.hashCode(id);
        result = prime * result + Double.hashCode(consumption);
        result = prime * result + measurementTimestamp.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Consumption{" +
                "id=" + id +
                ", measurementTimestamp=" + measurementTimestamp +
                ", consumption=" + consumption +
                '}';
    }
}
