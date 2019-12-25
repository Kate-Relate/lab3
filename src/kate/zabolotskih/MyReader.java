package kate.zabolotskih;

import org.jetbrains.annotations.NotNull;
import ru.spbstu.pipeline.Consumer;
import ru.spbstu.pipeline.Reader;
import ru.spbstu.pipeline.Status;
import ru.spbstu.pipeline.logging.Logger;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MyReader implements Reader {
    private static Set<String> TYPES = new HashSet<>();
    private static final String CHAR_ARRAY = char[].class.getCanonicalName();
    private static final String BYTE_ARRAY = byte[].class.getCanonicalName();
    private static final String STRING = String.class.getCanonicalName();
    static {
        TYPES.add(CHAR_ARRAY);
        TYPES.add(BYTE_ARRAY);
        TYPES.add(STRING);
    }

    private int sizeOfPart;
    private BufferedReader reader;
    private Logger logger;
    private Status status;
    private ArrayList<Consumer> consumers;
    private char[] data;

    public MyReader(String conf, Logger logger) {
        this.logger = logger;

        consumers = new ArrayList<Consumer>();

        MyReaderParser parser = new MyReaderParser(conf, logger);
        try {
            reader = new BufferedReader(new FileReader(parser.input));
            sizeOfPart = parser.getSizeOfPart();
        } catch (FileNotFoundException e) {
            status = Status.ERROR;
            logger.log("cannot open file to read (for Reader)");
        }
    }



    @Override
    public void run() {
        try {
            int size;
            char[] buf = new char[sizeOfPart];
            while ((size = reader.read(buf)) != -1) {
                data = new char[size];
                System.arraycopy(buf, 0, data, 0, size);
                for (Consumer c : consumers) {
                    if(c.loadDataFrom(this) != 0) {
                        c.run();
                        status = c.status();
                    }
                    else
                        status = Status.ERROR;

                    if (status != Status.OK)
                        return;
                }
            }
        } catch (IOException e) {
            status = Status.ERROR;
            logger.log("reading not passed");
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




    private final class ByteAccessor implements DataAccessor {
        int newSizeOfPart;

        @Override
        public @NotNull Object get() {
            byte[] newData = new String(data).getBytes(StandardCharsets.UTF_16BE);
            newSizeOfPart = newData.length;
            return newData;
        }

        @Override
        public long size() {
            return newSizeOfPart;
        }
    }

    private final class CharAccessor implements DataAccessor {
        @Override
        public @NotNull Object get() {
            return data.clone();
        }

        @Override
        public long size() {
            return data.length;
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




    public Status getStatus() {
        return status;
    }

    static class MyReaderParser extends GeneralParser {

        enum Grammar {
            INPUT("Input"),
            SIZE_OF_PART("SizeOfPart");

            String value;
            Grammar(String value) {
                this.value = value;
            }

            @Override
            public String toString() {
                return value;
            }
        }

        private String input;
        private int sizeOfPart;
        public MyReaderParser(String config, Logger logger) {
            super(config, logger);
            input = configParams.get(Grammar.INPUT.toString());
            sizeOfPart = Integer.decode(configParams.get(Grammar.SIZE_OF_PART.toString()));
        }

        public String getInput() {
            return input;
        }

        public int getSizeOfPart() {
            return sizeOfPart;
        }
    }
}
