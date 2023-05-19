package producer;

import api.Price;
import api.PriceProvider;
import api.Producer;
import com.google.common.collect.Iterables;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * This class provides an API to send prices towards PriceProviderService. <br>
 * The requests forwarded to the service are asynchronous, thus all methods immediately return. <br>
 * This class guarantees protection against logical mis-ordering of operations <br>
 * A producer instance may only engage in one batch upload at a time and user should start a new batch
 * only after completing or cancelling an ongoing batch. <br>
 */
public class PriceProducer implements Producer {
    private static final Logger LOGGER = Logger.getLogger(PriceProducer.class.getName());

    private final PriceProvider priceProvider;

    private String activeSubmissionId;
    private CompletionStage<Boolean> ongoingUploadFuture = CompletableFuture.completedFuture(false);

    public PriceProducer(PriceProvider priceProvider) {
        this.priceProvider = Objects.requireNonNull(priceProvider,
                "Can't initialize PriceProducer with a null PriceProvider");
    }

    /**
     * Initiate a new batch upload. This must be called before sending actual price data and always completes with true.
     */
    @Override
    public CompletionStage<Boolean> startNewBatch() {
        ongoingUploadFuture = initiateNewPriceUpload();
        return ongoingUploadFuture;
    }

    /**
     * Used to send price data to the service for a batch. <br>
     * If no batch upload was initiated, has already been completed or cancelled, completes with false. <br>
     * Can be called multiple times for a batch and sends data in chunks. <br>
     * Completes with true if batch upload (i.e. all chunks) succeeds, otherwise completes with false.
     */
    @Override
    public CompletionStage<Boolean> sendPrices(List<Price> prices) {
        ongoingUploadFuture = ongoingUploadFuture.thenCompose(initiateResult -> {
            if (activeSubmissionId == null) {
                //Not initiated, already completed, cancelled
                return CompletableFuture.completedFuture(false);
            }
            return uploadPriceDataBatch(prices);
        });
        return ongoingUploadFuture;
    }

    /**
     * Used to complete an ongoing batch upload. <br>
     * If no batch upload was initiated, has already been completed or cancelled, completes with false. <br>
     * In case the batch upload failed, completes with false and does not send any request towards service. <br>
     * If the batch upload succeeded, sends a request to complete the batch and returns the result. <br>
     * No new data for the batch can be sent if the completion request succeeds.
     */
    @Override
    public CompletionStage<Boolean> completePriceUpload() {
        ongoingUploadFuture = ongoingUploadFuture.thenCompose(uploadResult -> {
            if (activeSubmissionId == null) {
                return CompletableFuture.completedFuture(false);
            }
            if (uploadResult) {
                return applyTerminalOperation(this::sendBatchCompleteRequest,
                        String.format("Batch upload for submissionId %s successfully finished", activeSubmissionId));
            }
            return CompletableFuture.completedFuture(false);
        });
        return ongoingUploadFuture;
    }

    /**
     * Used to cancel an ongoing batch upload. <br>
     * If no batch upload was initiated, has already been completed or cancelled, completes with false. <br>
     * Cancels the batch regardless of if the batch upload succeeded or failed. <br>
     * Sends a request to cancel the batch to the service and returns the result.
     */
    @Override
    public CompletionStage<Boolean> cancelPriceUpload() {
        ongoingUploadFuture = ongoingUploadFuture.thenCompose(uploadResult -> {
            if (activeSubmissionId == null) {
                return CompletableFuture.completedFuture(false);
            }
            return applyTerminalOperation(this::sendBatchCancelRequest,
                    String.format("Successfully cancelled batch with submissionId %s", activeSubmissionId));
        });
        return ongoingUploadFuture;
    }

    private CompletionStage<Boolean> applyTerminalOperation(Supplier<CompletionStage<Boolean>> terminalOperation,
                                                            String successMessage) {
        return terminalOperation.get().thenApply(operationResult -> {
            if (operationResult) {
                LOGGER.info(successMessage);
                activeSubmissionId = null;
            }
            return operationResult;
        });
    }

    private CompletionStage<Boolean> initiateNewPriceUpload() {
        return priceProvider.startPriceUpload()
                .thenApply(receivedSubmissionId -> {
                    LOGGER.info(String.format("Batch upload for submissionId %s initiated", receivedSubmissionId));
                    activeSubmissionId = receivedSubmissionId;
                    return true;
                });
    }

    private CompletionStage<Boolean> uploadPriceDataBatch(List<Price> prices) {
        Iterator<List<Price>> priceChunkIterator = Iterables.partition(prices, 1000).iterator();
        CompletionStage<Boolean> chuckUploadResult = CompletableFuture.completedFuture(true);

        //Each chunk's result is chained with the next one if it's successful, otherwise false is propagated.
        while (priceChunkIterator.hasNext()) {
            List<Price> priceChunk = priceChunkIterator.next();
            chuckUploadResult = chuckUploadResult.thenCompose(chunkResult -> {
                if (chunkResult) {
                    return priceProvider.uploadPriceData(activeSubmissionId, priceChunk);
                }
                return CompletableFuture.completedFuture(false);
            });
        }

        //In case any intermediate chunk fails, entire batch fails. If all chunks succeeded, batch succeeds.
        return chuckUploadResult.thenCompose(batchResult -> {
            if (batchResult) {
                return CompletableFuture.completedFuture(true);
            }
            LOGGER.warning(String.format("Uploading price data failed for submissionId %s", activeSubmissionId));
            return CompletableFuture.completedFuture(false);
        });
    }

    private CompletionStage<Boolean> sendBatchCompleteRequest() {
        return priceProvider.completePriceUpload(activeSubmissionId);
    }

    private CompletionStage<Boolean> sendBatchCancelRequest() {
        return priceProvider.cancelPriceUpload(activeSubmissionId);
    }
}
