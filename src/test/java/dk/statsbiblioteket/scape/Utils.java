package dk.statsbiblioteket.scape;

public class Utils {

    public static String getMethodName() {
        return getCurrentStackTrace().getMethodName();
    }

    private static StackTraceElement getCurrentStackTrace() {
        return Thread.currentThread().getStackTrace()[3];
    }

    public static String getFullName() {
        final StackTraceElement currentStackTrace = getCurrentStackTrace();
        return currentStackTrace.getClassName()+"#"+currentStackTrace.getMethodName();
    }
}
