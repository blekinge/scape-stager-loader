package dk.statsbiblioteket.scape;

import com.google.common.util.concurrent.Futures;
import dk.statsbiblioteket.util.Strings;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

public class CheckoutClientTest {

    @Test
    public void testCheckoutEntitySimplest() throws Exception {
        System.out.println(Utils.getFullName());
        CheckoutClient checkoutClient = new CheckoutClient(null, null, null);

        final String testIdentifier = "testIdentifier";
        List<InputStream> resultStream
                = checkoutClient.checkoutEntities(identifier -> Futures.immediateFuture(new ByteArrayInputStream(
                                identifier.getBytes())
                                                                                       ), Arrays.asList(testIdentifier)
                                                 );

        Assert.assertEquals(testIdentifier.trim(), Strings.flush(resultStream.get(0)).trim());

    }


}