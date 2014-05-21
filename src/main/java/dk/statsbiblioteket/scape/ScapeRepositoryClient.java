package dk.statsbiblioteket.scape;

import com.google.common.io.ByteStreams;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

public class ScapeRepositoryClient {

    protected static final String CHECKOUT = "checkout";
    protected static final String COMMIT = "commit";
    protected static final String COMMAND = "command";
    protected static final String IDENTIFIER = "identifier";
    protected static final String FILE = "file";


    public static void main(String[] args) throws IOException, JAXBException {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("scape-stager-loader")
                                               .defaultHelp(true)
                                               .description(
                                                       "Checkout and commit of Scape Intellectual Entities in a Data Connector enabled Repository");
        parser.addArgument("--repo").required(true).help("The url to the repository data connector endpoint");
        parser.addSubparsers().dest(COMMAND).title("Commands").help("Select one of these sommands");
        Subparser checkout = parser.addSubparsers().addParser(CHECKOUT).help("The checkout command");
        Subparser commit = parser.addSubparsers().addParser(COMMIT).help("The commit command");

        checkout.addArgument(IDENTIFIER).nargs("*").help("Identifiers to checkout").required(true);
        commit.addArgument(FILE).nargs("*").help("Files to commit").required(true);
        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);

        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }

        final String repo = ns.getString("repo");
        switch (ns.getString(COMMAND)) {
            case CHECKOUT:

                checkout(repo, ns.getList(IDENTIFIER)).stream().forEach(stream -> {
                    try {
                        ByteStreams.copy(stream, System.out);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                break;
            case COMMIT:
                System.out.println("commit selected");
                commit(repo, ns.getList(FILE)).stream().forEach(System.out::println);
                break;
            default:
                System.err.printf("No command selected");
                System.exit(1);
        }
    }

    private static List<InputStream> checkout(String service, List<String> identifiers) {
        System.out.println(service);
        CheckoutClient client = new CheckoutClient(service,null,null);
        return client.checkoutEntity(identifiers);
    }

    private static List<String> commit(String service, List<String> files) throws JAXBException {
        System.out.println(service);
        CommitClient client = new CommitClient(service,null,null);
        return client.commitEntities(asFiles(files));
    }

    private static List<File> asFiles(List<String> files) {
        return files.parallelStream().map(File::new).collect(Collectors.toList());
    }


}
