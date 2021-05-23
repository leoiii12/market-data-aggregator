package info.leochoi;

import com.google.protobuf.Timestamp;
import com.google.type.Money;
import info.leochoi.MarketDataProtos.MarketData;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.observers.TestObserver;
import io.reactivex.rxjava3.schedulers.TestScheduler;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;

class MarketDataProcessorTest {

  @org.junit.jupiter.api.BeforeEach
  void setUp() {}

  @org.junit.jupiter.api.AfterEach
  void tearDown() {}

  @org.junit.jupiter.api.Test
  void newMessage_NewMarketDataMsg_simple() throws InterruptedException {
    final TestScheduler testScheduler = new TestScheduler();

    final MarketDataProcessor marketDataProcessor = new MarketDataProcessor(testScheduler);
    final Observable<MarketDataProtos.MarketData> marketDataObservable =
        marketDataProcessor.getMarketDataObservable();

    final TestObserver<MarketDataProtos.MarketData> testObserver =
        Observable.wrap(marketDataObservable).test();

    // ***********
    // 1
    // ***********
    marketDataProcessor.onMessage(
        new MarketDataProcessor.NewMarketDataMsg(getMarketData("DBX", 0)));
    testScheduler.advanceTimeBy(1000, TimeUnit.MILLISECONDS);
    Thread.sleep(1000);

    testObserver.assertValueAt(0, q -> q.getSymbol().equals("DBX") && q.getLast().getNanos() == 0);
    testObserver.assertValueCount(1);

    // ***********
    // 1
    // ***********
    marketDataProcessor.onMessage(
        new MarketDataProcessor.NewMarketDataMsg(getMarketData("DBX", 1)));
    testScheduler.advanceTimeBy(1000, TimeUnit.MILLISECONDS);
    Thread.sleep(1000);

    testObserver.assertValueAt(1, q -> q.getSymbol().equals("DBX") && q.getLast().getNanos() == 1);
    testObserver.assertValueCount(2);

    // ***********
    // 2
    // ***********
    marketDataProcessor.onMessage(
        new MarketDataProcessor.NewMarketDataMsg(getMarketData("DBX", 2)));
    testScheduler.advanceTimeBy(100, TimeUnit.MILLISECONDS);
    Thread.sleep(100);
    marketDataProcessor.onMessage(
        new MarketDataProcessor.NewMarketDataMsg(getMarketData("DBX", 3)));
    testScheduler.advanceTimeBy(1000, TimeUnit.MILLISECONDS);
    Thread.sleep(1000);

    testObserver.assertValueAt(2, q -> q.getSymbol().equals("DBX") && q.getLast().getNanos() == 2);
    testObserver.assertValueAt(3, q -> q.getSymbol().equals("DBX") && q.getLast().getNanos() == 3);
    testObserver.assertValueCount(4);
  }

  @org.junit.jupiter.api.Test
  void newMessage_NewMarketDataMsg_priority() throws InterruptedException {
    final TestScheduler testScheduler = new TestScheduler();

    final MarketDataProcessor marketDataProcessor = new MarketDataProcessor(testScheduler);
    final Observable<MarketDataProtos.MarketData> marketDataObservable =
        marketDataProcessor.getMarketDataObservable();

    final TestObserver<MarketDataProtos.MarketData> testObserver =
        Observable.wrap(marketDataObservable).test();

    final List<String> symbols_000_100 =
        TestConstants.symbols.stream().limit(100).collect(Collectors.toList());
    final List<String> symbols_200_300 =
        TestConstants.symbols.stream().skip(200).limit(100).collect(Collectors.toList());
    final String symbol_0 = TestConstants.symbols.get(0);
    final String symbol_1 = TestConstants.symbols.get(1);

    // ***********
    // Priority
    // ***********
    symbols_000_100.forEach(
        symbol -> {
          marketDataProcessor.onMessage(
              new MarketDataProcessor.NewMarketDataMsg(getMarketData(symbol, 0)));
        });

    marketDataProcessor.onMessage(
        new MarketDataProcessor.NewMarketDataMsg(getMarketData(symbol_0, 1)));
    marketDataProcessor.onMessage(
        new MarketDataProcessor.NewMarketDataMsg(getMarketData(symbol_0, 2)));
    marketDataProcessor.onMessage(
        new MarketDataProcessor.NewMarketDataMsg(getMarketData(symbol_0, 3)));
    marketDataProcessor.onMessage(
        new MarketDataProcessor.NewMarketDataMsg(getMarketData(symbol_1, 1)));

    testScheduler.advanceTimeBy(200, TimeUnit.MILLISECONDS);
    Thread.sleep(200);

    symbols_200_300.forEach(
        symbol -> {
          marketDataProcessor.onMessage(
              new MarketDataProcessor.NewMarketDataMsg(getMarketData(symbol, 1)));
        });

    testObserver.assertValueCount(100);

    testScheduler.advanceTimeBy(1000, TimeUnit.MILLISECONDS);
    Thread.sleep(1000);

    testObserver.assertValueCount(200);

    testScheduler.advanceTimeBy(1000, TimeUnit.MILLISECONDS);
    Thread.sleep(1000);

    testObserver.assertValueCount(202);
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
