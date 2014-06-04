package dk.statsbiblioteket.scape;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import eu.scape_project.util.ScapeMarshaller;

import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBException;
import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class CommitClient extends ScapeClient {

    private ScapeMarshaller marshaller;


    protected CommitClient(String service, String username, String password) throws JAXBException {
        super(service, username, password);
        marshaller = ScapeMarshaller.newInstance();
    }

    public List<String> commitEntity(java.util.function.Function<Entry<String, InputStream>, Future<String>> mapper,
                                     Iterable<Entry<String, InputStream>> files) {
        try {
            Collection<ListenableFuture<String>> result = new ArrayList<>();
            files.forEach(entry -> result.add(JdkFutureAdapters.listenInPoolThread(mapper.apply(entry))));
            return Futures.allAsList(result).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }


    public List<String> commitEntity(java.util.function.Function<Entry<String, InputStream>, Future<String>> mapper,
                                     Collection<Entry<String,Future<InputStream>>> files) {


        try {
            return Futures.allAsList(files.parallelStream()
                                          .map(this::parseEntity)
                                          .map(mapper::apply)
                                          .map(JdkFutureAdapters::listenInPoolThread)
                                          .collect(Collectors.toList())).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private Entry<String, InputStream> parseEntity(Entry<String,Future<InputStream>> futurestream) {



        try {
            InputStream stream = futurestream.getValue().get();

            BufferedInputStream bufferedStream = new BufferedInputStream(stream);
            return new Entry<>(futurestream.getKey(), bufferedStream);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> commitEntities(Collection<Entry<String,Future<InputStream>>> files) {
        return commitEntity(this::putIntellectualEntity, files);
    }

    public List<String> commitEntities(Iterable<Entry<String, InputStream>> files) {
        return commitEntity(this::putIntellectualEntity, files);
    }


    public List<String> addAndCommitEntities(Collection<Entry<String, Future<InputStream>>> files) {
        return commitEntity(this::postIntellectualEntity, files);
    }


    private Future<String> putIntellectualEntity(Entry<String, InputStream> entry) {
        return request().path(entry.getKey()).entity(entry.getValue(), MediaType.APPLICATION_XML_TYPE).put(String.class);
    }


    private Future<String> postIntellectualEntity(Entry<String, InputStream> entry) {
        return request().entity(entry.getValue(), MediaType.APPLICATION_XML_TYPE).post(String.class);
    }

}
