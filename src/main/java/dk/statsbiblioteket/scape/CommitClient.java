package dk.statsbiblioteket.scape;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import eu.scape_project.model.IntellectualEntity;
import eu.scape_project.util.ScapeMarshaller;

import javax.ws.rs.client.Entity;
import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
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
                                     Collection<File> files) {


        try {
            return Futures.allAsList(files.parallelStream()
                                          .map(this::parseEntity)
                                          .map(mapper::apply)
                                          .map(future -> JdkFutureAdapters.listenInPoolThread(future))
                                          .collect(Collectors.toList())).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private Entry<String, InputStream> parseEntity(File file) {
        try {
            return new Entry<>(marshaller.deserialize(IntellectualEntity.class, new FileInputStream(file))
                                         .getIdentifier()
                                         .getValue(), new FileInputStream(file));
        } catch (JAXBException | FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> commitEntities(Collection<File> files) {
        return commitEntity(this::putIntellectualEntity, files);
    }

    public List<String> addAndCommitEntities(Collection<File> files) {
        return commitEntity(this::postIntellectualEntity, files);
    }


    private Future<String> putIntellectualEntity(Entry<String, InputStream> entry) {
        return Futures.immediateFuture(request().path(entry.getKey())
                                                .request()
                                                .put(Entity.xml(entry.getValue()), String.class));
    }


    private Future<String> postIntellectualEntity(Entry<String, InputStream> entry) {
        return Futures.immediateFuture(request()
                                                .request()
                                                .post(Entity.xml(entry.getValue()), String.class));
    }

}
