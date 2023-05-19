package api;

import java.util.List;
import java.util.concurrent.CompletionStage;

public interface Producer {
    CompletionStage<Boolean> startNewBatch();

    CompletionStage<Boolean> sendPrices(List<Price> prices);

    CompletionStage<Boolean> completePriceUpload();

    CompletionStage<Boolean> cancelPriceUpload();
}
