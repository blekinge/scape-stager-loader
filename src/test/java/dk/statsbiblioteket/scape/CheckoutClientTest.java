package dk.statsbiblioteket.scape;

import com.google.common.util.concurrent.Futures;
import dk.statsbiblioteket.util.Strings;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class CheckoutClientTest {

    @Test
    public void testCheckoutEntitySimplest() throws Exception {
        System.out.println(Utils.getFullName());
        CheckoutClient checkoutClient = new CheckoutClient(null);

        final String testIdentifier = "testIdentifier";
        InputStream resultStream = checkoutClient.checkoutEntity(identifier -> {
                                                                     return Futures.immediateFuture(new ByteArrayInputStream(
                                                                                                            identifier
                                                                                                                    .getBytes())
                                                                                                   );
                                                                 }, testIdentifier
                                                                );

        Assert.assertEquals(Strings.flush(resultStream).trim(),"<glob>testIdentifier</glob>");

    }


}