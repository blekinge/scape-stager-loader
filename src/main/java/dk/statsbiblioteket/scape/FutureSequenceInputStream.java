package dk.statsbiblioteket.scape;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class FutureSequenceInputStream extends InputStream {
    Iterator<Future<? extends InputStream>> streams;
    InputStream in;


    public FutureSequenceInputStream(Future<? extends InputStream>... streams) {
        this(Arrays.asList(streams).iterator());
    }

    public FutureSequenceInputStream(
            Iterator<Future<? extends InputStream>> streams) {
        this.streams = streams;
        try {
            nextStream();
        } catch (IOException ex) {
            // This should never happen
            throw new Error("panic");
        }
    }

    /**
     * Continues reading in the next stream if an EOF is reached.
     */
    final void nextStream() throws IOException {
        if (in != null) {
            in.close();
        }

        if (streams.hasNext()) {
            Future<? extends InputStream> future = streams.next();
            try {
                in = future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IOException(e);
            }
            if (in == null) {
                throw new NullPointerException();
            }
        } else {
            in = null;
        }

    }

    public int read() throws IOException {
        if (in == null) {
            return -1;
        }
        int c = in.read();
        if (c == -1) {
            nextStream();
            return read();
        }
        return c;
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        if (in == null) {
            return -1;
        }

        int n = in.read(b, off, len);
        if (n <= 0) {
            nextStream();
            return read(b, off, len);
        }
        return n;
    }

    @Override
    public long skip(long n) throws IOException {
        if (in == null) {
            return -1;
        }
        long skipped = in.skip(n);
        if (skipped <= 0) {
            nextStream();
            return skip(n);
        }
        return skipped;

    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void close() throws IOException {
        do {
            nextStream();
        } while (in != null);
    }
}
