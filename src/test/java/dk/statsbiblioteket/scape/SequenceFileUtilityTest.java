package dk.statsbiblioteket.scape;

import dk.statsbiblioteket.scape.utilities.SequenceFileUtility;
import dk.statsbiblioteket.util.Strings;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;

public class SequenceFileUtilityTest {

    @Test
    public void testWriteAndRead() throws Exception{


        HashMap<String, InputStream> values = new HashMap<String, InputStream>();
        final String name = "testMets.xml";
        String before = Strings.flush(getResourceAsStream(name));
        values.put(name, getResourceAsStream(name));
        final File sequenceFile = new File("target/testsequencefile");
        sequenceFile.deleteOnExit();
        SequenceFileUtility.write(sequenceFile, values);

        Iterable<Entry<String, InputStream>> reads = SequenceFileUtility.read(sequenceFile);
        reads.forEach(
                result -> {
                    try {
                        Assert.assertEquals(before, Strings.flush(result.getValue()));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Test
    public void testWriteAndReadMultiple() throws Exception {


        HashMap<String, InputStream> values = new HashMap<String, InputStream>();
        final String name = "testMets.xml";
        final String name1 = "file1";
        String before1 = Strings.flush(getResourceAsStream(name));
        values.put(name1, getResourceAsStream(name));

        final String name2 = "file2";
        String before2 = Strings.flush(getResourceAsStream(name));
        values.put(name2, getResourceAsStream(name));


        final File sequenceFile = new File("target/testsequencefile");
        sequenceFile.deleteOnExit();
        SequenceFileUtility.write(sequenceFile, values);


        Iterator<Entry<String, InputStream>> reads = SequenceFileUtility.read(sequenceFile).iterator();

        Assert.assertEquals(before1, Strings.flush(reads.next().getValue()));
        Assert.assertEquals(before2, Strings.flush(reads.next().getValue()));
    }


    private InputStream getResourceAsStream(String name) {
        return Thread.currentThread().getContextClassLoader().getResourceAsStream(name);
    }


}