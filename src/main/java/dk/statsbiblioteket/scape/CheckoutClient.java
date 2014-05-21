package dk.statsbiblioteket.scape;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;

import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class CheckoutClient extends ScapeClient {

    protected CheckoutClient(String service, String username, String password) {
        super(service, username, password);
    }

    public List<InputStream> checkoutEntities(java.util.function.Function<String, Future<InputStream>> mapper,
                                              Collection<String> identifiers)  {
        try {
            return Futures.allAsList(identifiers.parallelStream()
                                         .map(mapper::apply)
                                         .map(future -> JdkFutureAdapters.listenInPoolThread(future))
                                         .collect(Collectors.toList())).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }


    }

    private Future<InputStream> getIntellectualEntity(String identifier) {
        return request().path(identifier).request().async().get(InputStream.class);
    }


    public List<InputStream> checkoutEntity(Collection<String> identifiers) {
        return checkoutEntities(this::getIntellectualEntity, identifiers);
    }

}
