package dk.statsbiblioteket.scape;

import com.google.common.io.ByteStreams;
import dk.statsbiblioteket.scape.utilities.FutureUtils;
import dk.statsbiblioteket.scape.utilities.SequenceFileUtility;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class ScapeRepositoryClient {

    protected static final String CHECKOUT = "checkout";
    protected static final String COMMIT = "commit";
    protected static final String COMMAND = "command";
    protected static final String IDENTIFIER = "identifier";
    protected static final String FILE = "file";
    private static final String SEQUENCEFILE = "sequenceFile";


    public static void main(String[] args) throws IOException, JAXBException {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("scape-stager-loader").defaultHelp(true).description(
                "Checkout and commit of Scape Intellectual Entities in a Data Connector enabled Repository");
        parser.addArgument("--repo").required(true).help("The url to the repository data connector endpoint");
        parser.addSubparsers().dest(COMMAND).title("Commands").help("Select one of these sommands");

        Subparser checkout = parser.addSubparsers().addParser(CHECKOUT).help("The checkout command");
        checkout.addArgument(IDENTIFIER).nargs("*").help("Identifiers to checkout").required(true);
        checkout.addArgument(SEQUENCEFILE).nargs(1).required(false).help(
                "Sequence file for the results. If not specified, will output to Std. out.");

        Subparser commit = parser.addSubparsers().addParser(COMMIT).help("The commit command");
        commit.addArgument(FILE).nargs("*").help("Files to commit").required(false);
        checkout.addArgument(SEQUENCEFILE).nargs(1).required(false).help(
                "Sequence file from where the files can be read.");


        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);

        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }

        final String repo = ns.getString("repo");
        String sequenceFile = ns.getString(SEQUENCEFILE);
        switch (ns.getString(COMMAND)) {
            case CHECKOUT:

                final List<String> identifiers = ns.getList(IDENTIFIER);

                final List<InputStream> checkouts = checkout(repo, identifiers);
                if (sequenceFile != null) {
                    SequenceFileUtility.write(new File(sequenceFile), map(identifiers, checkouts));
                } else {
                    checkouts.stream().forEach(
                            stream -> {
                                try {
                                    ByteStreams.copy(stream, System.out);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            });
                }
                break;
            case COMMIT:
                System.out.println("commit selected");
                if (sequenceFile != null) {
                    Iterable<Entry<String, InputStream>> entities = SequenceFileUtility.read(new File(sequenceFile));
                    commitFromSequenceFile(repo, entities).stream().forEach(System.out::println);
                } else {
                    commit(repo, ns.getList(FILE)).stream().forEach(System.out::println);
                }

                break;
            default:
                System.err.printf("No command selected");
                System.exit(1);
        }
    }

    private static <K, V> Map<K, V> map(List<K> keys, List<V> values) {
        HashMap<K, V> result = new HashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            result.put(keys.get(i), values.get(i));
        }
        return result;
    }

    private static List<InputStream> checkout(String service, List<String> identifiers) {
        System.out.println(service);
        CheckoutClient client = new CheckoutClient(service, null, null);
        return client.checkoutEntity(identifiers);
    }

    private static List<String> commit(String service, Collection<String> files) throws JAXBException {
        System.out.println(service);
        CommitClient client = new CommitClient(service, null, null);

        return client.commitEntities(FutureUtils.asFutureStreams(files.stream().map(File::new).collect(Collectors.toList())));
    }


    private static List<String> commitFromSequenceFile(String service, Iterable<Entry<String, InputStream>> files) throws JAXBException {
        System.out.println(service);
        CommitClient client = new CommitClient(service, null, null);

        return client.commitEntities(files);
    }


}
