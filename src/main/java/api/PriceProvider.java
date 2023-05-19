package api;

import java.util.List;
import java.util.concurrent.CompletionStage;

public interface PriceProvider {
    /**
     * Upon being provided an instrumentId, returns a completionStage that completes with the latest {@link Price}. <br>
     * The completionStage completes exceptionally with {@link PriceProviderException} in case the instrumentId is invalid. <br>
     */
    CompletionStage<Price> getLatestPrice(String instrumentId);

    /**
     * Used to inform the service about the intention to upload prices. This method must be called before uploading the actual data. <br>
     * The service returns a completionStage that completes with a unique submissionId that has to be used for each of the subsequent price uploads in the batch. <br>
     * The same id is also used to complete / cancel the batch, upon which it is invalidated. Thus, it can't be reused for another batch.
     */
    CompletionStage<String> startPriceUpload();

    /**
     * Takes as input a previously assigned submissionId and a set of {@link Price} data of maximum size 1000 and aggregates the batch. <br>
     * Successful submission returns true. Submitting invalid submissionId returns false.<br>
     * In case of an error in upload because of submitting a size more than 1000, false is returned
     * but previously submitted data is retained, until the batch is completed or cancelled by the producer. <br>
     */
    CompletionStage<Boolean> uploadPriceData(String submissionId, List<Price> prices);

    /**
     * Completes the ongoing batch upload associated with the provided submissionId and returns a completionStage having the completion status. <br>
     * In case the submission never started, has already finished or the submissionId is unrecognized, completionStage completes with false, else true. <br>
     * Completing the batch and updating the internal price data can take time because of ongoing modification of Price data.
     */
    CompletionStage<Boolean> completePriceUpload(String submissionId);

    /**
     * Cancels the ongoing batch upload associated with the provided submissionId and returns a completionStage that completes with the cancellation status. <br>
     * In case the submission never started, has already finished or the submissionId is unrecognized, completionStage completes with false, else true.
     */
    CompletionStage<Boolean> cancelPriceUpload(String submissionId);
}