package dk.statsbiblioteket.scape;

import dk.statsbiblioteket.util.Pair;

import java.io.InputStream;
import java.util.Collection;
import java.util.concurrent.Future;
import java.util.stream.Stream;

public class CheckoutClient extends ScapeClient {

    protected CheckoutClient(String service, String username, String password) {
        super(service, username, password);
    }

    public Stream<Pair<String, Future<InputStream>>> checkoutEntities(
            java.util.function.Function<String, Pair<String, Future<InputStream>>> mapper,
                                              Collection<String> identifiers) {
        return identifiers.parallelStream().map(mapper::apply);
    }

    private Pair<String, Future<InputStream>> getIntellectualEntity(String identifier) {
        return new Pair<>(identifier, request().path(identifier).get(InputStream.class));
    }


    public Stream<Pair<String, Future<InputStream>>> checkoutEntity(Collection<String> identifiers) {
        return checkoutEntities(this::getIntellectualEntity, identifiers);
    }
}
