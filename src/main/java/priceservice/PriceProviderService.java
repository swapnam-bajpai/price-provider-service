package priceservice;

import api.Price;
import api.PriceProvider;
import api.PriceProviderException;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

public class PriceProviderService implements PriceProvider {
    private static final Logger LOGGER = Logger.getLogger(PriceProviderService.class.getName());

    private static final int MAXIMUM_CHUNK_SIZE = 1000;
    private static final int CONSUMER_WAIT_DURATION_MILLIS = 100;
    private static final int PRODUCER_WAIT_DURATION_MILLIS = 100;
    private final Map<String, Map<String, Set<Price>>> batchUploadBySubmissionId = new ConcurrentHashMap<>();
    private final Map<String, BatchState> currentStateBySubmissionId = new ConcurrentHashMap<>();
    private final Map<String, Price> latestPriceByInstrumentId = new ConcurrentHashMap<>();

    private final ExecutorService priceTaskExecutor = Executors.newCachedThreadPool();

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);

    @Override
    public CompletableFuture<Price> getLatestPrice(String instrumentId) throws PriceProviderException {
        return CompletableFuture.supplyAsync(() -> readPriceInformationThreadSafe(
                Objects.requireNonNull(instrumentId, "Can't retrieve price for null instrumentId")
        ), priceTaskExecutor);
    }

    @Override
    public CompletionStage<String> startPriceUpload() {
        return CompletableFuture.supplyAsync(() -> {
            String submissionId = UUID.randomUUID().toString();
            currentStateBySubmissionId.put(submissionId, BatchState.STARTED);
            return submissionId;
        }, priceTaskExecutor);
    }

    @Override
    public CompletionStage<Boolean> uploadPriceData(String submissionId, List<Price> submittedPrices) {
        return CompletableFuture.supplyAsync(() -> {
            if (isInvalidUploadRequest(
                    Objects.requireNonNull(submittedPrices, "Can't upload data for null price dataset").size(),
                    Objects.requireNonNull(submissionId, "Can't upload data for null submissionId"))) {
                return false;
            }

            Map<String, Set<Price>> pricesByInstrumentId =
                    batchUploadBySubmissionId.computeIfAbsent(submissionId, id -> new ConcurrentHashMap<>());

            submittedPrices.forEach(submittedPrice -> pricesByInstrumentId.computeIfAbsent(
                            submittedPrice.id(),
                            id -> ConcurrentHashMap.newKeySet())
                    .add(submittedPrice));

            return true;
        }, priceTaskExecutor);
    }

    @Override
    public CompletionStage<Boolean> completePriceUpload(String submissionId) {
        return CompletableFuture.supplyAsync(() -> {
            if (isInvalidCompleteRequest(
                    Objects.requireNonNull(submissionId, "Can't complete batch for null submissionId"))) {
                return false;
            }
            //Write entire batch's data in one go to make all prices available at once
            writePriceInformationThreadSafe(batchUploadBySubmissionId.get(submissionId));
            removeBatchDataForId(submissionId);
            return true;
        }, priceTaskExecutor);
    }

    @Override
    public CompletionStage<Boolean> cancelPriceUpload(String submissionId) {
        return CompletableFuture.supplyAsync(() -> {
            if (isInvalidCancelRequest(
                    Objects.requireNonNull(submissionId, "Can't cancel batch for null submissionId"))) {
                return false;
            }
            removeBatchDataForId(submissionId);
            return true;
        }, priceTaskExecutor);
    }

    private Price readPriceInformationThreadSafe(String instrumentId) {
        try {
            while (!readWriteLock.readLock().tryLock()) {
                TimeUnit.MILLISECONDS.sleep(CONSUMER_WAIT_DURATION_MILLIS);
            }
            if (!latestPriceByInstrumentId.containsKey(instrumentId)) {
                throw new PriceProviderException(
                        String.format("No price data for instrumentId %s", instrumentId));
            }
            return latestPriceByInstrumentId.get(instrumentId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PriceProviderException("Reader thread interrupted while trying to acquire lock", e);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    private void writePriceInformationThreadSafe(Map<String, Set<Price>> pricesByInstrumentId) {
        try {
            while (!readWriteLock.writeLock().tryLock()) {
                TimeUnit.MILLISECONDS.sleep(PRODUCER_WAIT_DURATION_MILLIS);
            }
            pricesByInstrumentId.forEach((instrumentId, uploadedPrices) ->
                    uploadedPrices.forEach(uploadedPrice -> {
                        if (isValidNewPrice(instrumentId, uploadedPrice)) {
                            latestPriceByInstrumentId.put(instrumentId, uploadedPrice);
                        }
                    }));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PriceProviderException("Writer thread interrupted while trying to acquire lock", e);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    private void removeBatchDataForId(String submissionId) {
        currentStateBySubmissionId.remove(submissionId);
        batchUploadBySubmissionId.remove(submissionId);
    }

    private boolean isValidNewPrice(String instrumentId, Price newPrice) {
        return !latestPriceByInstrumentId.containsKey(instrumentId) ||
                latestPriceByInstrumentId.get(instrumentId).compareTo(newPrice) < 0;
    }

    private boolean isInvalidUploadRequest(int chunkSize, String submissionId) {
        return chunkSize > MAXIMUM_CHUNK_SIZE ||
                isInvalidTransition(submissionId, BatchState.UPLOADING);
    }

    private boolean isInvalidCompleteRequest(String submissionId) {
        return isInvalidTransition(submissionId, BatchState.COMPLETED);
    }

    private boolean isInvalidCancelRequest(String submissionId) {
        return isInvalidTransition(submissionId, BatchState.CANCELED);
    }

    /**
     * Check if the requested transition to the newState is valid for the given submission id, and if so, do so atomically and return false. <br>
     * In case the transition is invalid or there is no existing state to transition from, return true. <br>
     * This method effectively does a compare-and-set so that situations with multiple threads trying to update the state for
     * same submission id are correctly handled.
     */
    private boolean isInvalidTransition(String submissionId, BatchState newState) {
        try {
            return currentStateBySubmissionId.computeIfPresent(submissionId,
                    (id, oldState) -> {
                        if (oldState.isInvalidTransitionTo(newState)) {
                            throw new IllegalStateException(
                                    String.format("Can't transition to %s from %s", newState, oldState));
                        }
                        return newState;
                    }) == null;
        } catch (IllegalStateException e) {
            LOGGER.warning(e.getMessage());
            return true;
        }
    }

    private enum BatchState {
        STARTED,
        UPLOADING,
        COMPLETED,
        CANCELED;

        private static final Map<BatchState, Set<BatchState>> VALID_PRECEDING_STATES_BY_NEXT_STATE;

        static {
            VALID_PRECEDING_STATES_BY_NEXT_STATE = Map.of(
                    STARTED, Set.of(),
                    UPLOADING, Set.of(STARTED, UPLOADING),
                    COMPLETED, Set.of(STARTED, UPLOADING),
                    CANCELED, Set.of(STARTED, UPLOADING));
        }

        boolean isInvalidTransitionTo(BatchState nextState) {
            return !VALID_PRECEDING_STATES_BY_NEXT_STATE.get(nextState).contains(this);
        }
    }
}
