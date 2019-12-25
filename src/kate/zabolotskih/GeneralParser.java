package kate.zabolotskih;

import ru.spbstu.pipeline.Status;
import ru.spbstu.pipeline.logging.Logger;

import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Scanner;

public abstract class GeneralParser {
    protected static final String DIVIDER = " = ";
    protected static final int NAME = 0;
    protected static final int VALUE = 1;

    protected Status status;
    protected LinkedHashMap<String, String> configParams;

    public GeneralParser(String config, Logger logger) {
        status = Status.OK;
        configParams = new LinkedHashMap<String, String>();
        try {
            FileReader fileReader = new FileReader(config);
            Scanner sc = new Scanner(fileReader);

            readConfigFile(sc);

            fileReader.close();
        } catch (IOException e) {
            status = Status.ERROR;
            logger.log("cannot process config file of " + config);
        }
    }

    protected void readConfigFile(Scanner sc) {
        while (sc.hasNext()) {

            String[] str = sc.nextLine().split(DIVIDER);
            configParams.put(str[NAME], str[VALUE]);

        }
    }

    public Status getStatus() {
        return status;
    }
}
