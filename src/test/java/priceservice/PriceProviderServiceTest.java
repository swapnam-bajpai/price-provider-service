package priceservice;

import api.*;
import consumer.PriceConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import producer.PriceProducer;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

public class PriceProviderServiceTest {
    private static final AtomicLong PRICE_VALUE_UPDATER = new AtomicLong();

    private PriceProvider priceProviderService;
    private Producer producer;
    private Consumer consumer;

    @BeforeEach
    public void setup() {
        priceProviderService = new PriceProviderService();
        producer = new PriceProducer(priceProviderService);
        consumer = new PriceConsumer(priceProviderService);
    }

    @Test
    public void testProduceConsumePrices() {
        //Producer uploads prices for instruments 1,2,3 and completes batch
        LocalDateTime startTime = LocalDateTime.now();
        assertTrue(sendBatchAndWaitOnResult(producer, 3, startTime));

        //Consumer successfully retrieves price for instrument 2
        getAndAssertPrice(consumer, 2, startTime.plusSeconds(1));

        //Retrieval for instrument 4 fails since it is not uploaded yet
        assertRetrievalFailed(consumer, 4);

        //Producer updates prices for 4 instruments with later asOf times
        PRICE_VALUE_UPDATER.incrementAndGet();
        LocalDateTime laterTime = LocalDateTime.now();
        assertTrue(sendBatchAndWaitOnResult(producer, 4, laterTime));

        //Consumer successfully retrieves updated price for instrument 2
        getAndAssertPrice(consumer, 2, laterTime.plusSeconds(1));
        //Also retrieves for instrument 4
        getAndAssertPrice(consumer, 4, laterTime.plusSeconds(3));

        //Producer sends older batch of prices (with earlier asOf timestamps)
        PRICE_VALUE_UPDATER.incrementAndGet();
        LocalDateTime earlierTime = LocalDateTime.now().minusHours(1L);
        assertTrue(sendBatchAndWaitOnResult(producer, 4, earlierTime));

        //Consumer retrieves price from earlier batch with greater timestamp
        getAndAssertPriceValue(consumer, 2, laterTime.plusSeconds(1),
                PRICE_VALUE_UPDATER.longValue() - 1);
    }

    @Test
    void testProducerSendsBeforeNotifyingStart() {
        //Producer tries to send prices without starting batch first - Doesn't work
        LocalDateTime startTime = LocalDateTime.now();
        producer.sendPrices(getTestPrices(4, startTime));
        boolean batchStatus = producer.completePriceUpload().toCompletableFuture().join();
        assertFalse(batchStatus);

        //Consumer tries to retrieve price for Instrument 2 - Doesn't work
        assertRetrievalFailed(consumer, 2);
    }

    @Test
    void testConsumerReadsBeforeBatchCompletes() {
        //Producer starts a batch upload and sends some prices but doesn't notify finish
        LocalDateTime startTime = LocalDateTime.now();
        boolean uploadStatus = producer.startNewBatch().thenCompose(initiateResult -> {
            assertTrue(initiateResult);
            return producer.sendPrices(getTestPrices(4, startTime));
        }).toCompletableFuture().join();
        assertTrue(uploadStatus);

        //Consumer tries to retrieve price for Instrument 2 before batch has completed - Doesn't work
        assertRetrievalFailed(consumer, 2);

        //Producer notifies batch upload completion
        boolean batchStatus = producer.completePriceUpload().toCompletableFuture().join();
        assertTrue(batchStatus);

        //Consumer can now successfully retrieve price for Instrument 2
        getAndAssertPrice(consumer, 2, startTime.plusSeconds(1));
    }

    @Test
    void testProducerCancelsBatch() {
        //Producer starts a batch upload and sends some prices, but cancels without completing
        LocalDateTime startTime = LocalDateTime.now();
        boolean cancelStatus = producer.startNewBatch().thenCompose(initiateResult -> {
            assertTrue(initiateResult);
            return producer.sendPrices(getTestPrices(4, startTime));
        }).thenCompose(uploadResult -> {
            assertTrue(uploadResult);
            return producer.cancelPriceUpload();
        }).toCompletableFuture().join();
        assertTrue(cancelStatus);

        //Consumer tries to retrieve price for Instrument 2 for a cancelled batch - Doesn't work
        assertRetrievalFailed(consumer, 2);
    }

    @Test
    void testProducerSendsMultipleChunks() {
        LocalDateTime chunk1Time = LocalDateTime.now();
        LocalDateTime chunk2Time = LocalDateTime.now().plusMinutes(1);
        boolean batchStatus = producer.startNewBatch()
                .thenCompose((initiateResult) -> {
                    assertTrue(initiateResult);
                    return producer.sendPrices(getTestPrices(3, chunk1Time));
                }).thenCompose((chunk1Result) -> {
                    assertTrue(chunk1Result);
                    PRICE_VALUE_UPDATER.incrementAndGet();
                    return producer.sendPrices(getTestPrices(4, chunk2Time));
                }).thenCompose((chunk2Result) -> {
                    assertTrue(chunk2Result);
                    return producer.completePriceUpload();
                }).toCompletableFuture().join();
        assertTrue(batchStatus);

        //Consumer retrieves the latest price of Instrument 2 across both chunks
        getAndAssertPrice(consumer, 3, chunk2Time.plusSeconds(2));
    }

    @Test
    void testParallelConsumers() throws InterruptedException {
        //Producer uploads prices for instruments 1,2,3 and completes batch
        LocalDateTime startTime = LocalDateTime.now();
        assertTrue(sendBatchAndWaitOnResult(producer, 3, startTime));

        Consumer consumer2 = new PriceConsumer(priceProviderService);
        Consumer consumer3 = new PriceConsumer(priceProviderService);
        Consumer consumer4 = new PriceConsumer(priceProviderService);

        //Four consumers extract latest prices for same/different instruments in parallel
        ExecutorService consumerPool = Executors.newFixedThreadPool(4);
        List<Future<Price>> consumerFutures = consumerPool.invokeAll(List.of(
                () -> consumer.getLatestPriceForInstrument(getInstrumentId(1)).toCompletableFuture().join(),
                () -> consumer2.getLatestPriceForInstrument(getInstrumentId(1)).toCompletableFuture().join(),
                () -> consumer3.getLatestPriceForInstrument(getInstrumentId(2)).toCompletableFuture().join(),
                () -> consumer4.getLatestPriceForInstrument(getInstrumentId(3)).toCompletableFuture().join()
        ));

        List<Price> retrievedPrices = consumerFutures.stream()
                .map(future -> {
                    try {
                        return future.get();
                    } catch (Exception e) {
                        throw new AssertionError("Unexpected failure");
                    }
                }).toList();

        assertEquals(new Price(getInstrumentId(1), startTime, getDummyData()), retrievedPrices.get(0));
        assertEquals(new Price(getInstrumentId(1), startTime, getDummyData()), retrievedPrices.get(1));
        assertEquals(new Price(getInstrumentId(2), startTime.plusSeconds(1), getDummyData()), retrievedPrices.get(2));
        assertEquals(new Price(getInstrumentId(3), startTime.plusSeconds(2), getDummyData()), retrievedPrices.get(3));
    }

    @Test
    void testParallelProducers() throws InterruptedException {
        Producer producer2 = new PriceProducer(priceProviderService);
        Producer producer3 = new PriceProducer(priceProviderService);
        Producer producer4 = new PriceProducer(priceProviderService);

        LocalDateTime producer1Time = LocalDateTime.now();
        LocalDateTime producer2Time = LocalDateTime.now().plusMinutes(1);
        LocalDateTime producer3Time = LocalDateTime.now().plusMinutes(2);
        LocalDateTime producer4Time = LocalDateTime.now().minusMinutes(1);

        //Four producers publish prices for various instruments in parallel
        ExecutorService consumerPool = Executors.newFixedThreadPool(4);
        List<Future<Boolean>> producerFutures = consumerPool.invokeAll(List.of(
                () -> sendBatch(producer, 4, producer1Time).toCompletableFuture().join(),
                () -> sendBatch(producer2, 3, producer2Time).toCompletableFuture().join(),
                () -> sendBatch(producer3, 1, producer3Time).toCompletableFuture().join(),
                () -> sendBatch(producer4, 5, producer4Time).toCompletableFuture().join()
        ));

        producerFutures.parallelStream()
                .map(future -> {
                    try {
                        return future.get();
                    } catch (Exception e) {
                        throw new AssertionError("Unexpected failure");
                    }
                }).forEach(Assertions::assertTrue);

        //Consumer retrieves the latest prices for all instruments
        getAndAssertPrice(consumer, 1, producer3Time);
        getAndAssertPrice(consumer, 2, producer2Time.plusSeconds(1));
        getAndAssertPrice(consumer, 3, producer2Time.plusSeconds(2));
        getAndAssertPrice(consumer, 4, producer1Time.plusSeconds(3));
        getAndAssertPrice(consumer, 5, producer4Time.plusSeconds(4));
    }

    private static boolean sendBatchAndWaitOnResult(Producer producer, int priceCount, LocalDateTime startTime) {
        return sendBatch(producer, priceCount, startTime).toCompletableFuture().join();
    }

    private static CompletionStage<Boolean> sendBatch(Producer producer, int priceCount, LocalDateTime startTime) {
        return producer.startNewBatch()
                .thenCompose((initiateResult) -> {
                    assertTrue(initiateResult);
                    return producer.sendPrices(getTestPrices(priceCount, startTime));
                }).thenCompose((chunkResult) -> {
                    assertTrue(chunkResult);
                    return producer.completePriceUpload();
                });
    }

    private static void getAndAssertPrice(Consumer consumer, int id, LocalDateTime time) {
        getAndAssertPriceValue(consumer, id, time, PRICE_VALUE_UPDATER.longValue());
    }

    private static void getAndAssertPriceValue(Consumer consumer, int id, LocalDateTime time, long dataValue) {
        String retrievedId = getInstrumentId(id);
        Price retrievalResult = consumer.getLatestPriceForInstrument(retrievedId)
                .toCompletableFuture().join();
        assertEquals(new Price(retrievedId, time, getDummyData(dataValue)), retrievalResult);
    }

    private static void assertRetrievalFailed(Consumer consumer, int id) {
        String retrievedId = getInstrumentId(id);
        CompletableFuture<Price> failedRetrieval = consumer.getLatestPriceForInstrument(retrievedId)
                .toCompletableFuture()
                .whenComplete((price, exception) -> {
                    assertNull(price);
                    assertNotNull(exception);
                });
        waitForExceptionalFutureToFinish(failedRetrieval);
    }

    private static List<Price> getTestPrices(int priceCount,
                                             LocalDateTime startTime) {
        List<Price> prices = new ArrayList<>(priceCount);
        for (int idx = 1; idx <= priceCount; idx++) {
            prices.add(new Price(getInstrumentId(idx), startTime, getDummyData()));
            startTime = startTime.plusSeconds(1);
        }
        return prices;
    }

    private static String getInstrumentId(int id) {
        return String.format("Instrument:%d", id);
    }

    private static PriceData getDummyData() {
        return getDummyData(PRICE_VALUE_UPDATER.longValue());
    }

    private static PriceData getDummyData(long value) {
        return new PriceData(value);
    }

    private static void waitForExceptionalFutureToFinish(CompletableFuture<Price> future) {
        try {
            future.join();
        } catch (CompletionException e) {
            //Do nothing, expected to complete with exception
        }
    }
}
