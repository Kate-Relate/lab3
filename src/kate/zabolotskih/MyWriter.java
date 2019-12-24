package kate.zabolotskih;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import ru.spbstu.pipeline.Producer;
import ru.spbstu.pipeline.Status;
import ru.spbstu.pipeline.Writer;
import ru.spbstu.pipeline.logging.Logger;

import java.io.*;
import java.util.*;

public class MyWriter implements Writer {
    private final static String DATA_TYPE = char[].class.getCanonicalName();

    private Map<Producer, Producer.DataAccessor> producers;
    private BufferedWriter bufferedWriter;
    private Logger logger;
    private Status status;
    private char[] data;

    public MyWriter (String conf, Logger logger) {

        producers = new HashMap<>();

        this.logger = logger;

        MyWriterParser parser = new MyWriterParser(conf, logger);
        try {
            bufferedWriter = new BufferedWriter(new FileWriter(parser.output));

        } catch (IOException e) {
            status = Status.ERROR;
            logger.log("cannot open file to write");
        }
    }
    @Override
    public long loadDataFrom(Producer producer) {
        int size;
        char[] buf = (char []) producers.get(producer).get();
        if (buf.length != (size = (int)producers.get(producer).size())) {
            status = Status.ERROR;
            return 0;
        }
        data = new char[size];
        System.arraycopy(buf, 0,data, 0, size);
        return data.length;
    }

    @Override
    public void run() {
        try {
            bufferedWriter.write(data);
            bufferedWriter.flush();
        } catch (IOException e) {
            status = Status.ERROR;
            logger.log("writing not passed");
        }
    }

    @Override
    public Status status() {
        return status;
    }

    @Override
    public void addProducer(Producer producer) {
        Set<String> types = producer.outputDataTypes();

        if (types.contains(DATA_TYPE))
            producers.put(producer, producer.getAccessor(DATA_TYPE));
        else {
            status = Status.EXECUTOR_ERROR;
        }
    }

    @Override
    public void addProducers(List<Producer> list) {
        for (Producer p: list) {
            Set<String> types = p.outputDataTypes();

            if (types.contains(DATA_TYPE))
                producers.put(p, p.getAccessor(DATA_TYPE));
            else {
                status = Status.EXECUTOR_ERROR;
            }
        }
    }

    public Status getStatus() {
        return status;
    }

    static class MyWriterParser extends GeneralParser {

        enum Grammar {
            OUTPUT("Output");

            String value;
            Grammar(String value) {
                this.value = value;
            }
            @Override
            public String toString() {
                return value;
            }
        }
        private String output;

        public MyWriterParser(String config, Logger logger) {
            super(config, logger);
            output = configParams.get(Grammar.OUTPUT.toString());
        }

        public String getOutput() {
            return output;
        }
    }
}

