package obalitskyi.gridu.randomEventProducer;

import com.opencsv.CSVWriter;
import org.apache.commons.io.output.StringBuilderWriter;

import java.net.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;


public class SocketProducer implements Closeable {
    private SocketChannel channel = null;
    private ByteBuffer byteBuffer = null;
    private int batchSize = -1;
    private int eventsWritten = 0;
    private CSVWriter csvWriter = null;
    private StringBuilder builder = null;

    // constructor to put ipInfo.csv address and port
    public SocketProducer(String host, int port, int bufferSize, int eventSize) {
        // establish a connection
        try {
            InetSocketAddress hostAddress = new InetSocketAddress(host, port);
            channel = SocketChannel.open(hostAddress);
            channel.socket().setSendBufferSize(bufferSize);
            System.out.println("Connected");

            this.batchSize = (bufferSize / eventSize) - 1;
            byteBuffer = ByteBuffer.allocate(bufferSize);
            byteBuffer.clear();

            StringBuilderWriter stringWriter = new StringBuilderWriter();
            this.builder = stringWriter.getBuilder();
            csvWriter = new CSVWriter(stringWriter);
        } catch (IOException u) {
            u.printStackTrace();
        }
    }

    public void produce(String[] line) throws IOException {
        if (eventsWritten >= batchSize) {
            writeDown();
        }

        csvWriter.writeNext(line);
        String lineStr = builder.toString();
        builder.setLength(0);
        byteBuffer.put(lineStr.getBytes());
        eventsWritten++;
    }

    private void writeDown() throws IOException {
        byteBuffer.flip();
        while(byteBuffer.hasRemaining()) {
            channel.write(byteBuffer);
        }
        byteBuffer.clear();
        eventsWritten = 0;
    }

    public void close() {
        // close the connection
        try {
            writeDown();
            channel.close();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }
}
