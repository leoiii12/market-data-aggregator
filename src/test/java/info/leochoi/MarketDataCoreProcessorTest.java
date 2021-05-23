package info.leochoi;

import com.google.protobuf.Timestamp;
import com.google.type.Money;
import info.leochoi.MarketDataProtos.MarketData;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.observers.TestObserver;
import io.reactivex.rxjava3.schedulers.TestScheduler;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.jooq.lambda.tuple.Tuple2;

class MarketDataCoreProcessorTest {

  @org.junit.jupiter.api.BeforeEach
  void setUp() {}

  @org.junit.jupiter.api.AfterEach
  void tearDown() {}

  @org.junit.jupiter.api.Test
  void emitNewMarketData() {
    final TestScheduler testScheduler = new TestScheduler();

    final MarketDataCoreProcessor marketDataCoreProcessor =
        new MarketDataCoreProcessor(testScheduler);
    final Observable<MarketDataProtos.MarketData> marketDataObservable =
        marketDataCoreProcessor.getMarketDataObservable();

    final TestObserver<MarketDataProtos.MarketData> testObserver =
        Observable.wrap(marketDataObservable).test();

    marketDataCoreProcessor.emitNewMarketData(getMarketData("DBX", 1));
    testScheduler.advanceTimeBy(200, TimeUnit.MILLISECONDS);
    marketDataCoreProcessor.emitNewMarketData(getMarketData("DBX", 2));
    testScheduler.advanceTimeBy(200, TimeUnit.MILLISECONDS);
    marketDataCoreProcessor.emitNewMarketData(getMarketData("DBX", 3));
    testScheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);
    marketDataCoreProcessor.emitNewMarketData(getMarketData("DBX", 4));

    testObserver.assertValueAt(0, q -> q.getSymbol().equals("DBX") && q.getLast().getNanos() == 1);
    testObserver.assertValueAt(1, q -> q.getSymbol().equals("DBX") && q.getLast().getNanos() == 2);
    testObserver.assertValueAt(2, q -> q.getSymbol().equals("DBX") && q.getLast().getNanos() == 3);
    testObserver.assertValueAt(3, q -> q.getSymbol().equals("DBX") && q.getLast().getNanos() == 4);
  }

  @org.junit.jupiter.api.Test
  void getPriorityToSymbolTuple2Observable() {
    final TestScheduler testScheduler = new TestScheduler();

    final MarketDataCoreProcessor marketDataCoreProcessor =
        new MarketDataCoreProcessor(testScheduler);
    final Observable<Tuple2<Long, String>> dbxQuoteObservable =
        marketDataCoreProcessor
            .getPriorityToSymbolTuple2Observable()
            .filter(g -> "DBX".equals(g.v2()));
    final TestObserver<Tuple2<Long, String>> dbxMarketDataTestObserver =
        Observable.wrap(dbxQuoteObservable).test();

    // ***********
    // Emits 0 + 2
    // ***********
    testScheduler.advanceTimeTo(0, TimeUnit.MILLISECONDS);
    marketDataCoreProcessor.emitNewMarketData(getMarketData("DBX", 0)); // 0
    testScheduler.advanceTimeTo(200, TimeUnit.MILLISECONDS);
    marketDataCoreProcessor.emitNewMarketData(getMarketData("DBX", 1)); // 1
    testScheduler.advanceTimeTo(600, TimeUnit.MILLISECONDS);
    marketDataCoreProcessor.emitNewMarketData(getMarketData("DBX", 2)); // 2
    testScheduler.advanceTimeTo(1000, TimeUnit.MILLISECONDS);

    dbxMarketDataTestObserver.assertValueCount(2);

    // ***********
    // Emits 3
    // ***********
    testScheduler.advanceTimeTo(2000, TimeUnit.MILLISECONDS);
    marketDataCoreProcessor.emitNewMarketData(getMarketData("DBX", 3)); // 0

    dbxMarketDataTestObserver.assertValueAt(
        2,
        q ->
            q.v1() == 1
                && marketDataCoreProcessor.getLatestQuote(q.v2()).getSymbol().equals("DBX")
                && marketDataCoreProcessor.getLatestQuote(q.v2()).getLast().getNanos() == 3);
    dbxMarketDataTestObserver.assertValueCount(3);

    // ***********
    // Emits 4
    // ***********
    testScheduler.advanceTimeTo(2100, TimeUnit.MILLISECONDS);
    marketDataCoreProcessor.emitNewMarketData(getMarketData("DBX", 4)); // 1
    testScheduler.advanceTimeTo(3000, TimeUnit.MILLISECONDS);

    dbxMarketDataTestObserver.assertValueAt(
        2,
        q ->
            q.v1() == 1
                && marketDataCoreProcessor.getLatestQuote(q.v2()).getSymbol().equals("DBX")
                && marketDataCoreProcessor.getLatestQuote(q.v2()).getLast().getNanos() == 4);
    dbxMarketDataTestObserver.assertValueAt(
        3,
        q ->
            q.v1() == 2
                && marketDataCoreProcessor.getLatestQuote(q.v2()).getSymbol().equals("DBX")
                && marketDataCoreProcessor.getLatestQuote(q.v2()).getLast().getNanos() == 4);
    dbxMarketDataTestObserver.assertValueCount(4);

    // ***********
    // Emits 10 + 16
    // ***********
    testScheduler.advanceTimeTo(5000, TimeUnit.MILLISECONDS);
    marketDataCoreProcessor.emitNewMarketData(getMarketData("DBX", 10)); // 0

    dbxMarketDataTestObserver.assertValueAt(
        4,
        q ->
            q.v1() == 1
                && marketDataCoreProcessor.getLatestQuote(q.v2()).getSymbol().equals("DBX")
                && marketDataCoreProcessor.getLatestQuote(q.v2()).getLast().getNanos() == 10);

    testScheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);
    marketDataCoreProcessor.emitNewMarketData(getMarketData("DBX", 11)); // 1

    dbxMarketDataTestObserver.assertValueAt(
        4,
        q ->
            q.v1() == 1
                && marketDataCoreProcessor.getLatestQuote(q.v2()).getSymbol().equals("DBX")
                && marketDataCoreProcessor.getLatestQuote(q.v2()).getLast().getNanos() == 11);

    testScheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);
    marketDataCoreProcessor.emitNewMarketData(getMarketData("DBX", 12)); // 2

    dbxMarketDataTestObserver.assertValueAt(
        4,
        q ->
            q.v1() == 1
                && marketDataCoreProcessor.getLatestQuote(q.v2()).getSymbol().equals("DBX")
                && marketDataCoreProcessor.getLatestQuote(q.v2()).getLast().getNanos() == 12);

    testScheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);
    marketDataCoreProcessor.emitNewMarketData(getMarketData("DBX", 13)); // 3
    testScheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);
    marketDataCoreProcessor.emitNewMarketData(getMarketData("DBX", 14)); // 4
    testScheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);
    marketDataCoreProcessor.emitNewMarketData(getMarketData("DBX", 15)); // 5
    testScheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);
    marketDataCoreProcessor.emitNewMarketData(getMarketData("DBX", 16)); // 6
    testScheduler.advanceTimeBy(499, TimeUnit.MILLISECONDS);
    testScheduler.advanceTimeTo(6000, TimeUnit.MILLISECONDS);

    dbxMarketDataTestObserver.assertValueAt(
        5,
        q ->
            q.v1() == 7
                && marketDataCoreProcessor.getLatestQuote(q.v2()).getSymbol().equals("DBX")
                && marketDataCoreProcessor.getLatestQuote(q.v2()).getLast().getNanos() == 16);
    dbxMarketDataTestObserver.assertValueCount(6);
  }

  private MarketData getMarketData(final @NotNull String symbol, final int i) {
    final long millis = System.currentTimeMillis();
    final Timestamp timestamp =
        Timestamp.newBuilder()
            .setSeconds(millis / 1000)
            .setNanos((int) ((millis % 1000) * 1000000))
            .build();

    final MarketDataProtos.MarketData marketData =
        MarketDataProtos.MarketData.newBuilder()
            .setSymbol(symbol)
            .setBid(Money.newBuilder().setCurrencyCode("USD").setUnits(27).setNanos(0).build())
            .setAsk(Money.newBuilder().setCurrencyCode("USD").setUnits(27).setNanos(0).build())
            .setLast(Money.newBuilder().setCurrencyCode("USD").setUnits(27).setNanos(i).build())
            .setUpdateTime(timestamp)
            .build();

    return marketData;
  }
}
