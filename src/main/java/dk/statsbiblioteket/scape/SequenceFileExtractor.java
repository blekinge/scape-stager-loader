package dk.statsbiblioteket.scape;

import dk.statsbiblioteket.scape.utilities.SequenceFileUtility;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class SequenceFileExtractor {

    protected static final String FILE = "file";
    protected static final String DEST = "dest";
    protected static final String EXTRACT = "extract";
    protected static final String LIST = "list";

    public static void main(String[] args) throws IOException {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("sequence-file-extractor").defaultHelp(true);
        parser.addArgument(ScapeRepositoryClient.COMMAND).nargs(1).choices(EXTRACT, LIST).action(Arguments.store());
        parser.addArgument(FILE).nargs("+").required(true);
        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
        List<String> files = ns.getList(FILE);
        String dest = files.get(files.size() - 1);
        final String command = (String) ns.getList(ScapeRepositoryClient.COMMAND).get(0);
        switch (command) {
            case EXTRACT: {
                for (int i = 0; i < files.size() - 1; i++) {
                    String file = files.get(i);
                    extract(file, dest);
                }
                break;
            }
            case LIST: {
                list(dest);
                break;
            }
        }
    }


    public static void extract(String sequenceFile, String destinationDir) throws IOException {
        Iterable<Entry<String, InputStream>> contents = SequenceFileUtility.read(new File(sequenceFile));
        for (Entry<String, InputStream> content : contents) {
            File file = new File(destinationDir, content.getKey());
            file.getParentFile().mkdirs();
            try (OutputStream out = new FileOutputStream(file)) {
                IOUtils.copyLarge(content.getValue(), out);
            }
        }
    }


    public static void list(String sequenceFile) throws IOException {
        Iterable<Entry<String, InputStream>> contents = SequenceFileUtility.read(new File(sequenceFile));
        for (Entry<String, InputStream> content : contents) {
            System.out.println(content.getKey());
        }
    }
}
