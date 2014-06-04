package dk.statsbiblioteket.scape.utilities;

import dk.statsbiblioteket.scape.Entry;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class SequenceFileUtility {


    public static void write(File sequenceFile, Map<String, InputStream> values) throws IOException {
        Configuration conf = new Configuration(true);
        Text key = new Text();
        BytesWritable value = new BytesWritable();

        try (SequenceFile.Writer writer = SequenceFile.createWriter(
                conf,
                SequenceFile.Writer.file(new Path(sequenceFile.toURI())),
                SequenceFile.Writer.keyClass(key.getClass()),
                SequenceFile.Writer.valueClass(value.getClass()))) {
            for (Map.Entry<String, InputStream> entry : values.entrySet()) {
                final byte[] bytes = IOUtils.toByteArray(entry.getValue());
                value.set(bytes, 0, bytes.length);
                key.set(entry.getKey());
                writer.append(key, value);
            }
        }
    }

    public static Iterable<Entry<String, InputStream>> read(File sequenceFile) throws IOException {
        return new SequenceFileIterable(sequenceFile.toURI());
    }

    public static class SequenceFileIterable implements Iterable<Entry<String, InputStream>> {

        private URI sequenceFile;

        public SequenceFileIterable(URI sequenceFile) throws IOException {
            this.sequenceFile = sequenceFile;
        }

        @Override
        public SequenceFileIterator iterator() {
            return new SequenceFileIterator(sequenceFile);
        }

        public static class SequenceFileIterator implements Iterator<Entry<String,InputStream>> {

            private final org.apache.hadoop.io.SequenceFile.Reader reader;

            boolean haveRead = false;
            Text key = new Text();
            BytesWritable value = new BytesWritable();
            private boolean next;

            public SequenceFileIterator(URI sequenceFile) {
                Configuration conf = new Configuration(true);
                try {
                    reader = new org.apache.hadoop.io.SequenceFile.Reader(
                            conf, org.apache.hadoop.io.SequenceFile.Reader.file(new Path(sequenceFile)));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public synchronized boolean hasNext() {
                try {
                    if (!haveRead) {
                        haveRead = true;
                        next = reader.next(key, value);
                    }
                    return next;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            }

            @Override
            public synchronized Entry<String, InputStream> next() {
                if (hasNext()){
                    final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(value.copyBytes());
                    haveRead = false;
                    return new Entry<>(key.toString(),byteArrayInputStream);
                } else {
                    throw new NoSuchElementException();
                }

            }

            @Override
            protected void finalize() throws Throwable {
                super.finalize();
                reader.close();
            }
        }
    }
}
