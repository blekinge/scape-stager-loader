package dk.statsbiblioteket.scape;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import eu.scape_project.model.IntellectualEntity;

import javax.ws.rs.client.Entity;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class CommitClient implements ScapeClient {

    private String service;


    public List<String> commitEntity(java.util.function.Function<Entry<String, Entity<InputStream>>, Future<String>> mapper,
                                     Collection<IntellectualEntity> entities) {


        try {
            return Futures.allAsList(entities.parallelStream()
                                             .map(entity -> new Entry<>(entity.getIdentifier()
                                                                              .getValue(),
                                                                        XmlUtils.toBytes(entity, false)
                                             ))
                                             .map(pair -> new Entry<>(pair.getKey(), Entity.xml(pair.getValue())))
                                             .map(mapper::apply)
                                             .map(future -> JdkFutureAdapters.listenInPoolThread(future))
                                             .collect(Collectors.toList())).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

  /*  private Future<String> putIntellectualEntity(Pair<String, Entity<InputStream>> http) {
        return httpClient.target(service).path(http.getFirst()).request().async().put(http.getSecond(), String.class);
    }*/

}
