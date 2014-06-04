package dk.statsbiblioteket.scape.utilities;

import dk.statsbiblioteket.scape.ParsingException;
import eu.scape_project.model.IntellectualEntity;
import eu.scape_project.model.Representation;
import eu.scape_project.util.ScapeMarshaller;

import javax.xml.bind.JAXBException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class XmlUtils {
    private static ScapeMarshaller scapeMarshaller;


    public static IntellectualEntity toEntity(InputStream contents) throws ParsingException {
        try {
            return getScapeMarshaller().deserialize(IntellectualEntity.class, contents);
        } catch (JAXBException e) {
            throw new ParsingException(e);
        }
    }

    public static Representation toRepresentation(InputStream contents) throws ParsingException {
        try {
            return getScapeMarshaller().deserialize(Representation.class, contents);
        } catch (JAXBException e) {
            throw new ParsingException(e);
        }
    }

    public static <T> T toObject(String contents) throws ParsingException {
        return toObject(stream(contents));
    }

    private static InputStream stream(String contents) {
        return new ByteArrayInputStream(contents.getBytes());
    }

    public static <T> T toObject(InputStream contents) throws ParsingException {
        if (contents == null) {
            return null;
        }
        try {
            return (T) getScapeMarshaller().getJaxbUnmarshaller().unmarshal(contents);
        } catch (JAXBException e) {
            throw new ParsingException(e);
        }
    }

    private static ScapeMarshaller getScapeMarshaller() throws JAXBException {
        if (scapeMarshaller == null) {
            scapeMarshaller = ScapeMarshaller.newInstance();
        }
        return scapeMarshaller;

    }

    public static InputStream toBytes(Object datastream) throws ParsingException {
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        try {
            getScapeMarshaller().getJaxbMarshaller().marshal(datastream, sink);
        } catch (JAXBException e) {
            throw new ParsingException(e);
        }
        try {
            sink.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new ByteArrayInputStream(sink.toByteArray());
    }

    public static InputStream toBytes(IntellectualEntity entity, boolean useReferences) throws ParsingException {
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        try {

            getScapeMarshaller().serialize(entity, sink, useReferences);
        } catch (JAXBException e) {
            throw new ParsingException(e);
        }
        try {
            sink.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new ByteArrayInputStream(sink.toByteArray());
    }

    public static String toString(Object xml) throws ParsingException {
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        try {
            getScapeMarshaller().serialize(xml, sink);
        } catch (JAXBException e) {
            throw new ParsingException(e);
        }
        return sink.toString();
    }

    public static void toFile(Object xml, File file) throws IOException {
        try (FileOutputStream outStream = new FileOutputStream(file)){
            getScapeMarshaller().serialize(xml, outStream);
        } catch (JAXBException e) {
            throw new ParsingException(e);
        }

    }


}
