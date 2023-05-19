package consumer;

import api.Consumer;
import api.Price;
import api.PriceProvider;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * This class provides an API to request the latest prices from the PriceProviderService. <br>
 * The requests forwarded to the service are asynchronous and hence method calls immediately return. <br>
 **/
public class PriceConsumer implements Consumer {
    private final PriceProvider priceProvider;

    public PriceConsumer(PriceProvider priceProvider) {
        this.priceProvider = Objects.requireNonNull(priceProvider,
                "Can't initialize PriceConsumer with a null PriceProvider");
    }

    @Override
    public CompletionStage<Price> getLatestPriceForInstrument(String instrumentId) {
        return priceProvider.getLatestPrice(instrumentId);
    }
}
