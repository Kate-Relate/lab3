package kate.zabolotskih.pipeline;

import kate.zabolotskih.MyReader;
import ru.spbstu.pipeline.*;
import ru.spbstu.pipeline.logging.Logger;

import java.util.ArrayList;

public class Pipeline {
    private Status status;
    private Reader reader;
    private ArrayList<Executor> executors;
    private Writer writer;


    public Pipeline(String confFile, Logger logger) {
        executors = new ArrayList<Executor>();
        PipelineParser pp = new PipelineParser(confFile, logger);
        status = pp.getStatus();
        if (pp.getStatus() != Status.OK) {
            return;
        }
        writer = pp.getWriter();
        executors.addAll(pp.getExecutors());
        reader = pp.gerReader();

        reader.addConsumer(executors.get(0));
        executors.get(0).addProducer(reader);
        status = ((MyReader)executors.get(0)).getStatus();
        for (int i = 0; i < executors.size() - 1; i++) {
            executors.get(i).addConsumer(executors.get(i+1));
            executors.get(i+1).addProducer(executors.get(i));
            status = ((MyReader)executors.get(i+1)).getStatus();
        }
        writer.addProducer(executors.get(executors.size()-1));
        status = ((MyReader)executors.get(executors.size()-1)).getStatus();
        executors.get(executors.size()-1).addConsumer(writer);
    }

    public Status getStatus() {
        return status;
    }

    public void run() {
        reader.run();
    }
}
