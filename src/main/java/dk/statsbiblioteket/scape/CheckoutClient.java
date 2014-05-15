package dk.statsbiblioteket.scape;

import com.google.common.util.concurrent.Futures;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class CheckoutClient implements ScapeClient {


    private final String service;

    public CheckoutClient(String service) {
        this.service = service;
    }

    public InputStream checkoutEntity( java.util.function.Function<String, Future<InputStream>> mapper, String... identifiers) {
        List<Future<? extends InputStream>> entities = new ArrayList<>();
        entities.add(Futures.immediateFuture(new ByteArrayInputStream("<glob>".getBytes())));
        entities.addAll(Arrays.asList(identifiers)
                              .parallelStream()
                              .map(mapper::apply)
                              .collect(Collectors.toList()));
        entities.add(Futures.immediateFuture(new ByteArrayInputStream("</glob>".getBytes())));
        return new FutureSequenceInputStream(entities.iterator());
    }

    private Future<InputStream> getIntellectualEntity(String identifier) {
        return httpClient.target(service).path(identifier).request().async().get(InputStream.class);
    }

}
