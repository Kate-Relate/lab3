package kate.zabolotskih;


import ru.spbstu.pipeline.logging.Logger;

import java.io.*;
import java.util.logging.Level;

public class MyLogger implements Logger {

    private static FileWriter log;

    MyLogger(String fileName) {
         try {
             log = new FileWriter(fileName);
         } catch (IOException e) {
             System.out.println("File open error: Bye log...");
             return;
         }
    }

    @Override
    public void log(String s) {
        try {
            log.write(s);
            log.append('\n');
            log.flush();
        } catch (IOException e) {
            System.out.println("Can't write down the message: " + s + "\n");
            return;
        }
    }

    @Override
    public void log(String s, Throwable throwable) {
        try {
            log.write(s);
            log.append('\n');
            log.write(throwable.toString());
            log.append('\n');
            log.flush();
        } catch (IOException e) {
            System.out.println("Can't write down the message: " + s + " "  + throwable.toString() + "\n");
            return;
        }
    }

    @Override
    public void log(Level level, String s) {
        try {
            log.write("Error on level: " + level.toString());
            log.append('\n');
            log.write("Error: " + s);
            log.append('\n');
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Can't write down the message: " + s + " " + level.toString() + "\n");
        }
    }

    @Override
    public void log(Level level, String s, Throwable throwable) {
        try {
            log.write("Error on level: " + level.toString());
            log.append('\n');
            log.write("Error: " + s);
            log.append('\n');
            log.write(throwable.toString());
            log.append('\n');
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Can't write down the message: " + level.toString() + " " + s + " " + throwable.toString() + "\n");
        }
    }
}
