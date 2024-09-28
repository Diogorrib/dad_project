package dadkvs.server;

public class BackOff {

    private static long INITIAL_BACKOFF_DELAY_MS = 1000; // Initial delay in  milliseconds

    private static long MAX_BACKOFF_DELAY_MS = 30000; // Maximum delay in milliseconds

    private static double BACKOFF_MULTIPLIER = 2.0; //  backoff multiplier

    private static final int  MAX_ATTEMPTS = 1000;

    private int curr_attempt;

    public BackOff() {
        curr_attempt = 0;
    }

    //Function used in the implementation of an exponential backoffDelay in order to improve performance
    public long calculateBackoffDelay() {
        //Calculate new backoffDelay
        long backoffDelay = (long) (INITIAL_BACKOFF_DELAY_MS * Math.pow(BACKOFF_MULTIPLIER, curr_attempt));
        if (curr_attempt < MAX_ATTEMPTS) {
            curr_attempt++;
        }
        return Math.min(backoffDelay, MAX_BACKOFF_DELAY_MS);
    }

    public int getMaxAttempts() {
        return MAX_ATTEMPTS;
    }

    public void setInitialBackoffDelayMs(long initialBackoffDelayMs) {
        INITIAL_BACKOFF_DELAY_MS = initialBackoffDelayMs;
    }

    public void setMaxBackoffDelayMs(long maxBackoffDelayMs) {
        MAX_BACKOFF_DELAY_MS = maxBackoffDelayMs;
    }

    public void setBackoffMultiplier(long backoffMultiplier) {
        BACKOFF_MULTIPLIER = backoffMultiplier;
    }

}
