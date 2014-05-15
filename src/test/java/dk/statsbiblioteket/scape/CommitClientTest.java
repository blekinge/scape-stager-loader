package dk.statsbiblioteket.scape;

import com.google.common.util.concurrent.Futures;
import eu.scape_project.model.Identifier;
import eu.scape_project.model.IntellectualEntity;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class CommitClientTest {

    @Test
    public void testCommitEntity() throws Exception {
        System.out.println(Utils.getFullName());

        CommitClient commitClient = new CommitClient();

        final String testIdentifier = "testIdentifier";
        IntellectualEntity e = new IntellectualEntity.Builder().identifier(new Identifier(testIdentifier)).representations(
                Arrays.asList()).build();


        List<String> results = commitClient.commitEntity(pair -> {
            return Futures.immediateFuture(pair.getKey());
        }, Arrays.asList(e));

        results.forEach(result -> Assert.assertEquals(testIdentifier, result));

    }
}