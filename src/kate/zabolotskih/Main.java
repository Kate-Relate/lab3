package kate.zabolotskih;

import ru.spbstu.pipeline.Status;
import ru.spbstu.pipeline.logging.Logger;
import kate.zabolotskih.pipeline.Pipeline;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class Main {

    public static void main(String[] args) {
        Logger logger = new MyLogger("log.txt");

        Pipeline pipeline = new Pipeline("configs/config.txt", logger);
        if (pipeline.getStatus() != Status.OK)
            return;
        pipeline.run();
    }
}
