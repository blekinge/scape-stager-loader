package dk.statsbiblioteket.scape;

public class ParsingException extends MyException {
    public ParsingException(Exception e) {
        super(e);
    }

    public ParsingException() {
        super();
    }

    public ParsingException(String s) {
        super(s);
    }
}
