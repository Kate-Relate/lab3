package kate.zabolotskih;

import org.jetbrains.annotations.NotNull;
import ru.spbstu.pipeline.Consumer;
import ru.spbstu.pipeline.Executor;
import ru.spbstu.pipeline.Producer;
import ru.spbstu.pipeline.Status;
import ru.spbstu.pipeline.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class KatesExecutor implements Executor {
    private final static String DATA_TYPE = byte[].class.getCanonicalName();
    private static Set<String> TYPES = new HashSet<>();
    private static final String CHAR_ARRAY = char[].class.getCanonicalName();
    private static final String BYTE_ARRAY = byte[].class.getCanonicalName();
    private static final String STRING = String.class.getCanonicalName();
    static {
        TYPES.add(CHAR_ARRAY);
        TYPES.add(BYTE_ARRAY);
        TYPES.add(STRING);
    }
    private ArrayList<Consumer> consumers;
    private Map<Producer, DataAccessor> producers;

    private byte[] table;
    private Status status;
    byte[] data;


    public KatesExecutor(String conf, Logger logger) {
        consumers = new ArrayList<Consumer>();
        producers = new HashMap<>();

        status = Status.OK;
        KatesParser parser = new KatesParser(conf, logger);
        if (parser.getStatus() != Status.OK) {
            status = parser.getStatus();
            return;
        }
        String tableFile = parser.getTable();
        TableParser tableParser = new TableParser(tableFile, logger);
        if (tableParser.getStatus() != Status.OK) {
            status = parser.getStatus();
            return;
        }
        table = tableParser.getTable();

    }

    @Override
    public long loadDataFrom(@NotNull Producer producer) {
        int size;
        byte[] buf = (byte []) producers.get(producer).get();
        if (buf.length != (size = (int)producers.get(producer).size())) {
            status = Status.ERROR;
            return 0;
        }
        data = new byte[size];
        System.arraycopy(buf, 0,data, 0, size);
        return data.length;
    }

    @Override
    public @NotNull DataAccessor getAccessor(@NotNull String s) {
        if (s.equals(CHAR_ARRAY)) {
            return new CharAccessor();
        } else if (s.equals(BYTE_ARRAY)) {
            return new ByteAccessor();
        } else {
            return new StringAccessor();
        }
    }

    @Override
    public @NotNull Set<String> outputDataTypes() {
        return TYPES;
    }

    @Override
    public void run() {
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (table[data[i]] - Byte.MIN_VALUE);
        }

        for (Consumer consumer: consumers) {
            if(consumer.loadDataFrom(this) != 0) {
                consumer.run();
                status = consumer.status();
                if (status != Status.OK)
                    return;
            } else {
                status = consumer.status();
                return;
            }

        }
    }

    @Override
    public void addProducer(Producer producer) {
        Set<String> types = producer.outputDataTypes();

        if (types.contains(DATA_TYPE))
            producers.put(producer, producer.getAccessor(DATA_TYPE));
        else
            status = Status.EXECUTOR_ERROR;
    }

    @Override
    public void addProducers(List<Producer> list) {

        for (Producer producer : list) {
            addProducer(producer);
        }
    }

    @Override
    public void addConsumer(Consumer consumer) {
        consumers.add(consumer);
    }

    @Override
    public void addConsumers(List<Consumer> list) {
        consumers.addAll(list);
    }

    @Override
    public Status status() {
        return status;
    }

    static class TableParser extends GeneralParser {
        static final int SIZE = 256;
        private byte[] table;
        TableParser(String confFile, Logger logger) {
            super(confFile, logger);

            table = new byte[SIZE];
            for (int i = Byte.MIN_VALUE; i < Byte.MAX_VALUE; i++) {
                table[i - Byte.MIN_VALUE] = (byte) (i);
            }
            for (Map.Entry entry : configParams.entrySet()) {
                table[(byte)(((String) entry.getKey()).charAt(0))] =
                        (byte)((((String) entry.getValue()).charAt(0)) - Byte.MIN_VALUE);
            }
        }

        byte[] getTable() {
            return table;
        }
    }




    private final class ByteAccessor implements DataAccessor {

        @Override
        public @NotNull Object get() {
            return data;
        }

        @Override
        public long size() {
            return data.length;
        }
    }

    private final class CharAccessor implements DataAccessor {
        int newSize;
        @Override
        public @NotNull Object get() {
            char[] newData = new String(data, StandardCharsets.UTF_16BE).toCharArray();
            newSize = newData.length;
            return newData;
        }

        @Override
        public long size() {
            return newSize;
        }
    }

    private final class StringAccessor implements DataAccessor {
        int newSizeOfPart;

        @Override
        public @NotNull Object get() {
            String newData = new String(data);
            newSizeOfPart = newData.length();
            return newData;
        }

        @Override
        public long size() {
            return newSizeOfPart;
        }
    }



    static class KatesParser extends GeneralParser {
        enum Grammar {
            TABLE_FILE("TableFile");

            String value;
            Grammar(String value) {
                this.value = value;
            }


            @Override
            public String toString() {
                return value;
            }
        }

        private String table;

        KatesParser(String confFile, Logger logger) {
            super(confFile, logger);

            table = configParams.get(Grammar.TABLE_FILE.toString());

        }

        public String getTable() {
            return table;
        }
    }
}
