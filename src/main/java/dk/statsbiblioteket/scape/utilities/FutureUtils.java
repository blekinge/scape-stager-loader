package dk.statsbiblioteket.scape.utilities;

import dk.statsbiblioteket.scape.Entry;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FutureUtils {

    public static Collection<Entry<String, Future<InputStream>>> asFutureStreams(Iterable<File> files) {
        Collection<Entry<String, Future<InputStream>>> result = new ArrayList<>();
        files.forEach(file -> result.add(new Entry<>(file.getName().replaceAll("\\..*$", ""),
                new LazyFileFuture(file.getAbsolutePath()))));
        return result;
    }

    public static class LazyFileFuture implements Future<InputStream> {

        private final String fileIdentifier;
        InputStream stream = null;

        public LazyFileFuture(String fileIdentifier) {
            this.fileIdentifier = fileIdentifier;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return stream != null;
        }

        @Override
        public InputStream get() throws InterruptedException, ExecutionException {
            try {
                if (stream == null) {
                    stream = new FileInputStream(fileIdentifier);
                }
            } catch (IOException e) {
                throw new ExecutionException(e);
            }
            return stream;
        }

        @Override
        public InputStream get(long timeout, TimeUnit unit) throws
                                                            InterruptedException,
                                                            ExecutionException,
                                                            TimeoutException {
            return get();
        }
    }
}
