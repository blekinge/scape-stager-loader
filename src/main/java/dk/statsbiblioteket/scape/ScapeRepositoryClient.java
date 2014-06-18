package dk.statsbiblioteket.scape;

import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.Futures;
import dk.statsbiblioteket.scape.utilities.FutureUtils;
import dk.statsbiblioteket.scape.utilities.SequenceFileUtility;
import dk.statsbiblioteket.util.Pair;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentGroup;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.hadoop.io.SequenceFile;

import javax.xml.bind.JAXBException;
import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ScapeRepositoryClient {

    protected static final String CHECKOUT = "checkout";
    protected static final String COMMIT = "commit";
    protected static final String COMMAND = "command";
    protected static final String IDENTIFIER = "identifier";
    protected static final String FILE = "file";
    private static final String SEQUENCEFILE_IN = "commitSequenceFile";
    private static final String SEQUENCEFILE_OUT = "checkoutSequenceFile";
    private static final String IDENTIFIER_FILE = "id_file";


    public static void main(String[] args) throws IOException, JAXBException {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("scape-stager-loader")
                                               .defaultHelp(true)
                                               .description(
                                                       "Checkout and commit of Scape Intellectual Entities in a Data Connector enabled Repository");
        parser.addArgument("--repo").required(true).help("The url to the repository data connector endpoint");
        parser.addArgument("--user").required(true).help("The username to use when connecting to the repository");
        parser.addArgument(COMMAND).nargs(1).choices(CHECKOUT, COMMIT).action(Arguments.store());
        final ArgumentGroup checkout_group = parser.addArgumentGroup(CHECKOUT);
        checkout_group.addArgument("--" + IDENTIFIER).nargs("*").help("Identifiers to checkout").required(false);
        checkout_group.addArgument("--" + IDENTIFIER_FILE).required(false).help("Identifiers to checkout");
        checkout_group.addArgument("--" + SEQUENCEFILE_OUT)
                      .required(false)
                      .help("Sequence where the checked out objects will be written. If not specified, will output to Std. out.");
        final ArgumentGroup commit_group = parser.addArgumentGroup(COMMIT);
        commit_group.addArgument("--" + FILE).nargs("*").help("Files to commit").required(false);
        commit_group.addArgument("--" + SEQUENCEFILE_IN)
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
        final String command = (String) ns.getList(COMMAND).get(0);
        switch (command) {
            case CHECKOUT: {
                String sequenceFile = ns.getString(SEQUENCEFILE_OUT);
                String identifierFile = ns.getString(IDENTIFIER_FILE);
                List<String> identifiers = null;
                if (identifierFile == null) {
                    identifiers = ns.getList(IDENTIFIER);
                } else {
                    identifiers = Files.readAllLines(Paths.get(identifierFile))
                                       .stream()
                                       .map(String::trim)
                                       .collect(Collectors.toList());
                }
                CheckoutClient client = new CheckoutClient(repo, username, password);
                try {
                    final Stream<Pair<String, Future<InputStream>>> checkouts = checkout(client, identifiers);
                    if (sequenceFile != null) {
                        try (SequenceFile.Writer writer = SequenceFileUtility.openWriter(new File(sequenceFile))) {
                            checkouts.forEachOrdered(entry -> SequenceFileUtility.append(writer,
                                    entry.getLeft(),
                                    Futures.getUnchecked(entry.getRight())));
                        }
                    } else {
                        checkouts.forEach(entry -> {
                            try {
                                ByteStreams.copy(Futures.getUnchecked(entry.getRight()), System.out);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
                    }
                } finally {
                    client.destroy();
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

    private static Stream<Pair<String, Future<InputStream>>> checkout(CheckoutClient client, List<String> identifiers) {
        return client.checkoutEntity(identifiers);
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
