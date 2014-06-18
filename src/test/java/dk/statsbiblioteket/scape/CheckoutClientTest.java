package dk.statsbiblioteket.scape;

import com.google.common.util.concurrent.Futures;
import dk.statsbiblioteket.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.Future;
import java.util.stream.Stream;

public class CheckoutClientTest {

    @Test
    public void testCheckoutEntitySimplest() throws Exception {
        System.out.println(Utils.getFullName());
        CheckoutClient checkoutClient = new CheckoutClient(null, null, null);

        final String testIdentifier = "testIdentifier";
        Stream<Pair<String, Future<InputStream>>> resultStream
                = checkoutClient.checkoutEntities(identifier -> new Pair<>(identifier,
                        Futures.immediateFuture(new ByteArrayInputStream(identifier.getBytes()))),
                Arrays.asList(testIdentifier));
        Assert.assertEquals(testIdentifier.trim(), resultStream.findFirst().get().getRight().get());

    }


}