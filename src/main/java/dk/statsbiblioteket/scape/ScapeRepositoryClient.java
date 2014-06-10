package dk.statsbiblioteket.scape;

import com.google.common.io.ByteStreams;
import dk.statsbiblioteket.scape.utilities.FutureUtils;
import dk.statsbiblioteket.scape.utilities.SequenceFileUtility;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import javax.xml.bind.JAXBException;
import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ScapeRepositoryClient {

    protected static final String CHECKOUT = "checkout";
    protected static final String COMMIT = "commit";
    protected static final String COMMAND = "command";
    protected static final String IDENTIFIER = "identifier";
    protected static final String FILE = "file";
    private static final String SEQUENCEFILE_IN = "commitSequenceFile";
    private static final String SEQUENCEFILE_OUT = "checkoutSequenceFile";


    public static void main(String[] args) throws IOException, JAXBException {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("scape-stager-loader")
                                               .defaultHelp(true)
                                               .description(
                                                       "Checkout and commit of Scape Intellectual Entities in a Data Connector enabled Repository");
        parser.addArgument("--repo").required(true).help("The url to the repository data connector endpoint");
        parser.addArgument("--user").required(true).help("The username to use when connecting to the repository");
        parser.addSubparsers().dest(COMMAND).title("Commands").help("Select one of these commands");
        parser.addSubparsers().addParser(CHECKOUT).help("The checkout command");
        parser.addArgument("--" + IDENTIFIER).nargs("*").help("Identifiers to checkout").required(false);
        parser.addArgument("--" + SEQUENCEFILE_OUT)
              .nargs(1)
              .required(false)
              .help("Sequence where the checked out objects will be written. If not specified, will output to Std. out.");
        parser.addSubparsers().addParser(COMMIT).help("The commit command");
        parser.addArgument("--" + FILE).nargs("*").help("Files to commit").required(false);
        parser.addArgument("--" + SEQUENCEFILE_IN)
              .nargs(1)
              .required(false)
              .help("Sequence file to read the objects to be committed from.");
        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
        String password = "fedoraAdminPass";
        Console cons;
        if ((cons = System.console()) != null) {
            char[] passwd = cons.readPassword("[%s]", "Password:");
            if (passwd != null) {
                password = new String(passwd);
                java.util.Arrays.fill(passwd, ' ');
            }
        }
        final String username = ns.getString("user");
        final String repo = ns.getString("repo");
        switch (ns.getString(COMMAND)) {
            case CHECKOUT: {
                String sequenceFile = ns.getString(SEQUENCEFILE_OUT);
                final List<String> identifiers = ns.getList(IDENTIFIER);
                final List<InputStream> checkouts = checkout(repo, username, password, identifiers);
                if (sequenceFile != null) {
                    SequenceFileUtility.write(new File(sequenceFile), map(identifiers, checkouts));
                } else {
                    checkouts.stream().forEach(stream -> {
                        try {
                            ByteStreams.copy(stream, System.out);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                }
                break;
            }
            case COMMIT: {
                System.out.println("commit selected");
                String sequenceFile = ns.getString(SEQUENCEFILE_IN);
                if (sequenceFile != null) {
                    Iterable<Entry<String, InputStream>> entities = SequenceFileUtility.read(new File(sequenceFile));
                    commitFromSequenceFile(repo, username, password, entities).stream().forEach(System.out::println);
                } else {
                    commit(repo, username, password, ns.getList(FILE)).stream().forEach(System.out::println);
                }
                break;
            }
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

    private static List<InputStream> checkout(String service, String username, String password,
                                              List<String> identifiers) {
        System.out.println(service);
        CheckoutClient client = new CheckoutClient(service, username, password);
        try {
            return client.checkoutEntity(identifiers);
        } finally {
            client.destroy();
        }
    }

    private static List<String> commit(String service, String username, String password,
                                       Collection<String> files) throws JAXBException {
        System.out.println(service);
        CommitClient client = new CommitClient(service, username, password);
        try {
            return client.commitEntities(FutureUtils.asFutureStreams(files.stream()
                                                                          .map(File::new)
                                                                          .collect(Collectors.toList())));
        } finally {
            client.destroy();
        }
    }


    private static List<String> commitFromSequenceFile(String service, String username, String password,
                                                       Iterable<Entry<String, InputStream>> files) throws
                                                                                                   JAXBException {
        System.out.println(service);
        CommitClient client = new CommitClient(service, username, password);
        try {
            return client.commitEntities(files);
        } finally {
            client.destroy();
        }
    }
}
