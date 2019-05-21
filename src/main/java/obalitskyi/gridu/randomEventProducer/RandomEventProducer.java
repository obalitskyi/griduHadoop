package obalitskyi.gridu.randomEventProducer;

import java.io.Closeable;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class RandomEventProducer implements Closeable {
    private String[] line = new String[5];
    private Generator generator = new Generator();
    private SocketProducer socketProducer;
    private long numMessages = -1;

    public static void main(String[] args) throws IOException {
        String num = args[0];

        RandomEventProducer randomEventProducer = new RandomEventProducer();
        randomEventProducer.init(Long.valueOf(num), args[1]);
        randomEventProducer.generateLines();
        randomEventProducer.close();
    }

    public void init(Long numMessages, String filePath) throws IOException {
        Properties properties = new Properties();
        properties.load(getClass().getClassLoader().getResourceAsStream("flume.properties"));
        String host = properties.getProperty("host");
        int port = Integer.valueOf(properties.getProperty("port"));

        int bufferSize = Integer.parseInt(properties.getProperty("buffer.size.bytes"));
        int eventSize = Integer.parseInt(properties.getProperty("event.avrg.size.bytes"));
        socketProducer = new SocketProducer(host, port, bufferSize, eventSize);
        if (filePath == null) {
            filePath = properties.getProperty("ipsPath");
        }
        generator.readIps(new FileReader(filePath));

        if (numMessages == null) {
            this.numMessages = Long.parseLong(properties.getProperty("numMessages"));
        } else {
            this.numMessages = numMessages;
        }

    }

    public void generateLines() throws IOException {
        for (int i = 0; i < numMessages; i++) {

            line[0] = generator.generateProduct();
            line[1] = String.valueOf(generator.generatePrice());
            line[2] = generator.generateDate().toString();
            line[3] = generator.generateCategory();
            line[4] = generator.generateIpAddress();

            socketProducer.produce(line);
        }
    }

    public void close() {
        socketProducer.close();
        System.out.println("closed");
    }
}
