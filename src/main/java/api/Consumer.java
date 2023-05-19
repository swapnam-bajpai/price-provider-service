package api;

import java.util.concurrent.CompletionStage;

public interface Consumer {
    CompletionStage<Price> getLatestPriceForInstrument(String instrumentId);
}
