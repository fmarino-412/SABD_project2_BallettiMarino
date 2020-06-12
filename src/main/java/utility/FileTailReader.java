package utility;

import java.io.*;
import java.nio.file.*;
import java.util.ArrayDeque;

@SuppressWarnings({"ResultOfMethodCallIgnored", "BusyWait"})
public class FileTailReader {

    private final File file;
    private long tooLateTime = -1;
    private final long maxMillisecondsToWait;
    private boolean ended = false;
    private long offset = 0;
    private WatchService watchService = null;
    ArrayDeque<String> lines = new ArrayDeque<>();

    /**
     * Reads a file whose content can be updated by other processes
     * @param file to read
     * @param maxSecondsToWait max timeout after watching will stop, if 0 watching will continue until
     *                         <code>stop()</code> is called.
     */
    public FileTailReader(File file, long maxSecondsToWait) {
        this.file = file;
        this.maxMillisecondsToWait = maxSecondsToWait * 1000;
    }

    /**
     * Start watching file
     */
    public void start() {
        updateOffset();
        // listen for local filesystem events
        new Thread(new FileWatcher()).start();

        if (maxMillisecondsToWait != 0) {
            new Thread(new ServiceExpirationWatcher()).start();
        }
    }

    /**
     * Stop watching file
     */
    public void stop() {
        if (watchService != null) {
            try {
                watchService.close();
            } catch (IOException ignored) {
                System.out.println("Error closing watch service");
            }
            finally {
                watchService = null;
            }
        }
    }

    /**
     * Updates lines structure and offset
     */
    private synchronized void updateOffset() {
        tooLateTime = System.currentTimeMillis() + maxMillisecondsToWait;
        try {
            FileReader reader = new FileReader(file);
            BufferedReader br = new BufferedReader(reader);
            br.skip(offset);
            String line;
            while ((line = br.readLine()) != null) {
                // adds new line to the beginning
                lines.push(line);
                // adds the offset of the line and the terminator char
                offset += line.length() + 1;
            }
            br.close();
            reader.close();
        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
        }
    }

    /**
     * Tells if any new available line exists
     * @return true if new line exists, false elsewhere
     */
    public boolean linesAvailable() {
        return !lines.isEmpty();
    }

    /**
     * Used to get one new line
     * @return new line
     */
    public synchronized String getLine() {
        if (lines.isEmpty()) {
            return null;
        } else {
            // returns and removes older line
            return lines.removeLast();
        }
    }

    /**
     * Used to assert if no more lines will be available
     * @return true if stop() has been called or the service has expired
     */
    public boolean hasNotEnded() {
        return !ended;
    }

    /**
     * Watches for folder updates
     */
    @SuppressWarnings("BusyWait")
    private class FileWatcher implements Runnable {
        private final Path path = file.toPath().getParent();
        @Override
        public void run() {
            try {
                watchService = path.getFileSystem().newWatchService();
                path.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
                while (true) {
                    WatchKey watchKey = watchService.take();
                    if (!watchKey.reset()) {
                        stop();
                        break;
                    } else if (!watchKey.pollEvents().isEmpty()) {
                        updateOffset();
                    }
                    Thread.sleep(500);
                }
            } catch (InterruptedException | IOException | ClosedWatchServiceException ignored) {
                System.out.println("\u001B[32m" +
                        "File watcher stopped... There is no new data, computation is reaching the end" + "\u001B[0m");
            } finally {
                ended = true;
            }
        }
    }

    /**
     * Determines whether watching service has expired or not
     */
    private class ServiceExpirationWatcher implements Runnable {
        @Override
        public void run() {
            while (System.currentTimeMillis() < tooLateTime) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
            }
            stop();
        }
    }

    public static void test(String[] args) throws InterruptedException {
        FileTailReader reader = new FileTailReader(new File("/Users/francescomarino/Desktop/prova.txt"), 20);
        reader.start();
        while (reader.hasNotEnded()) {
            while (reader.linesAvailable()) {
                System.out.println(reader.getLine());
            }
            Thread.sleep(1000);
        }
    }
}
