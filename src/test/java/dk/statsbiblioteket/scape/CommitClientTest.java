package dk.statsbiblioteket.scape;

import dk.statsbiblioteket.scape.utilities.FutureUtils;
import dk.statsbiblioteket.scape.utilities.XmlUtils;
import eu.scape_project.model.Identifier;
import eu.scape_project.model.IntellectualEntity;
import eu.scape_project.model.LifecycleState;
import eu.scape_project.model.Representation;
import eu.scape_project.util.ScapeMarshaller;
import info.lc.xmlns.textmd_v3.TextMD;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class CommitClientTest {

    protected static final String SERVICE = "http://localhost:18080/scape-doms-data-connector/entity/";
    protected static final String USERNAME = "fedoraAdmin";
    protected static final String PASSWORD = "fedoraAdminPass";
    private ScapeMarshaller scapeMarshaller;
    private CommitClient commitClient;
    private String testIdentifier;
    private CheckoutClient checkoutClient;
    private IntellectualEntity e;

    public static TextMD createTextMDRecord() {
        TextMD textMd = new TextMD();
        TextMD.Encoding enc = new TextMD.Encoding();
        TextMD.Encoding.EncodingPlatform pf = new TextMD.Encoding.EncodingPlatform();
        pf.setValue("value");
        pf.setLinebreak("LF");
        enc.getEncodingPlatform().add(pf);
        textMd.getEncoding().add(enc);
        return textMd;
    }

    @Before
    public void setUp() throws Exception {
        scapeMarshaller = ScapeMarshaller.newInstance();
        commitClient = new CommitClient(SERVICE, USERNAME, PASSWORD);
        checkoutClient = new CheckoutClient(SERVICE, USERNAME, PASSWORD);
        testIdentifier = "" + new Random().nextLong();
        e = new IntellectualEntity.Builder().identifier(new Identifier(testIdentifier))
                                            .lifecycleState(new LifecycleState("", LifecycleState.State.NEW))
                                            .representations(Arrays.asList(new Representation.Builder().identifier(new Identifier(
                                                                    "Representation"))
                                                                                                       .title("representation")
                                                                                                       .technical(
                                                                                                               "SCAPE_REPRESENTATION_TECHNICAL",
                                                                                                               createTextMDRecord())
                                                                                                       .files(Arrays.asList(
                                                                                                                       new eu.scape_project.model.File.Builder()
                                                                                                                               .technical(
                                                                                                                                       "JPYLYZER",
                                                                                                                                       createTextMDRecord())
                                                                                                                               .identifier(
                                                                                                                                       new Identifier(
                                                                                                                                               "The_One_File"))
                                                                                                                               .build()))
                                                                                                       .build()))
                                            .build();
        scapeMarshaller.serialize(e, System.out);
    }

    @Test
    public void testCommitEntitySimplest() throws Exception {
        System.out.println(Utils.getFullName());
        File tempfile = File.createTempFile("entity", "dfs");
        FileOutputStream outStream = new FileOutputStream(tempfile);
        scapeMarshaller.serialize(e, outStream);
        outStream.close();
        List<String> results = commitClient.addAndCommitEntities(FutureUtils.asFutureStreams(Arrays.asList(tempfile)));
        results.forEach(result -> Assert.assertEquals(e.getIdentifier().getValue() + "", result));
    }


    @Test
    public void testCommitReadUpdate() throws Exception {
        System.out.println(Utils.getFullName());
        File tempfile = File.createTempFile("entity", "dfs");
        tempfile.deleteOnExit();
        XmlUtils.toFile(e, tempfile);
        List<String> results = commitClient.addAndCommitEntities(FutureUtils.asFutureStreams(Arrays.asList(tempfile)));
        results.forEach(result -> Assert.assertEquals(e.getIdentifier().getValue() + "", result));
        List<InputStream> checkoutStream = checkoutClient.checkoutEntity(Arrays.asList(testIdentifier));
        IntellectualEntity entityRead = XmlUtils.toEntity(checkoutStream.get(0));
        Assert.assertEquals(e, entityRead);
    }
}