package obalitskyi.gridu.randomEventProducer;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.joda.time.DateTime;

import java.io.InputStreamReader;
import java.util.*;

// Not thread safe
// hardcoded config values
public class Generator {
    private Random uniform = new Random();
    private Random gaussianPrice = new Random();
    private Random gaussianTime = new Random();

    private short alphabetSize = 3;
    private short productWordSize = 5;
    private short categoryWordSize = 3;

    private double minPrice = 10;
    private double maxPrice = 10000;
    private double meanPrice = 5005;
    private double devPrice = 2000;

    private int minSeconds = 0;
    // end of day
    private int maxSeconds = 86399999;
    // most purchases are made in the noon
    private int meanTime = 43199999;
    // most purchases are made from 9 am to 3 pm, e.g. 6 hours
    private int devTime = 21600000;

    private DateTime start = DateTime.parse("2019-05-01");
    private DateTime end = DateTime.parse("2019-05-07T23:59:59.999");
    private short daysRange = 7;

    private StringBuilder buffer = new StringBuilder();

    private List<String> ips = new ArrayList<>();

    public void readIps(InputStreamReader inputStreamReader) {
        CSVReader csvReader = new CSVReaderBuilder(inputStreamReader)
                .withSkipLines(1).build();
        csvReader.forEach(line -> {
            ips.add(line[0]);
        });
    }

    public String generateProduct() {
        buffer.setLength(0);
        for (short i = 0; i < productWordSize; i++) {
            buffer.append((char) (uniform.nextInt(alphabetSize) + 'a'));
        }

        return buffer.toString();
    }

    public String generateCategory() {
        buffer.setLength(0);
        for (short i = 0; i < categoryWordSize; i++) {
            buffer.append((char) (uniform.nextInt(alphabetSize) + 'a'));
        }
        return buffer.toString();
    }

    public String generateIpAddress() {
        return ips.get(uniform.nextInt(ips.size()));
    }

    public double generatePrice() {
        double price;
        double g;
        do {
            g = gaussianPrice.nextGaussian();
            price = g * devPrice + meanPrice;
        } while (price < minPrice || price > maxPrice);
        return price;
    }

    public DateTime generateDate() {
        DateTime date;
        do {
            int time = (int) (gaussianTime.nextGaussian() * devTime + meanTime);
            date = start.plusDays(uniform.nextInt(daysRange)).plusMillis(time);
        } while (date.isBefore(start) || date.isAfter(end));
        return date;
    }
}
