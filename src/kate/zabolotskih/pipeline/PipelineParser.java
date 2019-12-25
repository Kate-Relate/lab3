package kate.zabolotskih.pipeline;

import kate.zabolotskih.GeneralParser;
import ru.spbstu.pipeline.Executor;
import ru.spbstu.pipeline.Status;
import ru.spbstu.pipeline.logging.Logger;
import ru.spbstu.pipeline.Writer;
import ru.spbstu.pipeline.Reader;

import java.lang.reflect.Constructor;
import java.util.*;

public class PipelineParser extends GeneralParser {

    public enum Grammar {
        EXECUTOR("Executor"),
        EXECUTOR_CONFIG("ExecutorConfig"),
        WRITER("Writer"),
        WRITER_CONFIG("WriterConfig"),
        READER("Reader"),
        READER_CONFIG("ReaderConfig");

        private String title;
        Grammar(String title) {
            this.title = title;
        }

        @Override
        public String toString() {
            return title;
        }
    }
    Status status = Status.OK;

    ArrayList<Executor> executors;
    Reader reader;
    Writer writer;

    PipelineParser(String file, Logger logger) {
        super(file, logger);

        executors = new ArrayList<Executor>();

        Iterator iterator = configParams.entrySet().iterator();

        Map.Entry line = (Map.Entry) iterator.next();
        String keyName = (String) line.getKey();
        String className = (String) line.getValue();
        line = (Map.Entry) iterator.next();
        String configName = (String) line.getKey();
        String configFile = (String) line.getValue();
        if(!(putReaderFromConfig(keyName, className, logger, configName, configFile)))
            return;

        line = (Map.Entry) iterator.next();
        keyName = (String) line.getKey();
        className = (String) line.getValue();
        line = (Map.Entry) iterator.next();
        configName = (String) line.getKey();
        configFile = (String) line.getValue();
        if (!(putWriterFromConfig(keyName, className, logger, configName, configFile)))
            return;

        while (iterator.hasNext()) {
            line = (Map.Entry) iterator.next();
            keyName = (String) line.getKey();
            className = (String) line.getValue();
            if (!keyName.startsWith(Grammar.EXECUTOR.toString())) {
                status = Status.ERROR;
                logger.log("Incorrect general config (executor lines)");
            }
            line = (Map.Entry) iterator.next();
            configName = (String) line.getKey();
            configFile = (String) line.getValue();
            if (!configName.startsWith(Grammar.EXECUTOR_CONFIG.toString())) {
                status = Status.ERROR;
                logger.log("Incorrect general config (executor lines)");
                return;
            }
            try {
                Class c = Class.forName(className);
                Constructor[] constructors = c.getConstructors();
                Executor e = (Executor) constructors[0].newInstance(configFile, logger);
                executors.add(e);
            } catch (Exception e) {
                status = Status.ERROR;
                logger.log("Problem with a name of executor class");
                return;
            }
        }
    }

    private boolean putReaderFromConfig(String keyName, String className, Logger logger, String configName, String configFile) {
        if (!keyName.equals(Grammar.READER.toString())) {
            status = Status.ERROR;
            logger.log("Incorrect general config (reader line)");
        }
        if (!configName.equals(Grammar.READER_CONFIG.toString())) {
            status = Status.ERROR;
            logger.log("Incorrect general config (reader line)");
            return false;
        }
        try {
            Class c = Class.forName(className);
            Constructor[] constructors = c.getConstructors();
            reader = (Reader) constructors[0].newInstance(configFile, logger);
        } catch (Exception e) {
            status = Status.ERROR;
            logger.log("Problem with the name of reader class");
            return false;
        }
        return true;
    }

    private boolean putWriterFromConfig(String keyName, String className, Logger logger, String configName, String configFile) {
            if (!keyName.equals(Grammar.WRITER.toString())) {
                status = Status.ERROR;
                logger.log("Incorrect general config (writer line)");
            }
            if (!configName.equals(Grammar.WRITER_CONFIG.toString())) {
                status = Status.ERROR;
                logger.log("Incorrect general config (writer line)");
                return false;
            }
            try {
                Class c = Class.forName(className);
                Constructor[] constructors = c.getConstructors();
                writer = (Writer) constructors[0].newInstance(configFile, logger);
            } catch (Exception e) {
                status = Status.ERROR;
                logger.log("Problem with a name of writer class");
                return false;
            }
            return true;
    }

    @Override
    protected void readConfigFile(Scanner sc) {
        int i = 0;
        while (sc.hasNext()) {

            String[] str = sc.nextLine().split(DIVIDER);
            if (str[NAME].equals(Grammar.EXECUTOR.toString())) {
                str[NAME] += String.valueOf(i++);
            } else if (str[NAME].equals(Grammar.EXECUTOR_CONFIG.toString())) {
                str[NAME] += String.valueOf(i);
            }
            configParams.put(str[NAME], str[VALUE]);
        }
    }

    public ArrayList<Executor> getExecutors() {
        return executors;
    }
    public Reader getReader() {
        return reader;
    }
    public Writer getWriter() {
        return writer;
    }
}
