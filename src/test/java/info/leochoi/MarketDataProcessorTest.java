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
  void emitNewMarketData_simple() {
    final TestScheduler testScheduler = new TestScheduler();

    final MarketDataProcessor marketDataProcessor = new MarketDataProcessor(testScheduler);
    final Observable<MarketDataProtos.MarketData> marketDataObservable =
        marketDataProcessor.getMarketDataObservable();

    final TestObserver<MarketDataProtos.MarketData> testObserver =
        Observable.wrap(marketDataObservable).test();

    // ***********
    // Simple
    // ***********
    testScheduler.advanceTimeTo(0, TimeUnit.MILLISECONDS);
    marketDataProcessor.onMessage(
        new MarketDataProcessor.NewMarketDataMsg(getMarketData("DBX", 0)));

    testObserver.assertValueAt(0, q -> q.getSymbol().equals("DBX") && q.getLast().getNanos() == 0);
    testObserver.assertValueCount(1);

    testScheduler.advanceTimeTo(200, TimeUnit.MILLISECONDS);
    marketDataProcessor.onMessage(
        new MarketDataProcessor.NewMarketDataMsg(getMarketData("DBX", 1)));

    testObserver.assertValueAt(0, q -> q.getSymbol().equals("DBX") && q.getLast().getNanos() == 0);
    testObserver.assertValueCount(1);

    testScheduler.advanceTimeTo(1000, TimeUnit.MILLISECONDS);

    testObserver.assertValueAt(0, q -> q.getSymbol().equals("DBX") && q.getLast().getNanos() == 0);
    testObserver.assertValueCount(2);
  }

  @org.junit.jupiter.api.Test
  void emitNewMarketData_over() {
    final TestScheduler testScheduler = new TestScheduler();

    final MarketDataProcessor marketDataProcessor = new MarketDataProcessor(testScheduler);
    final Observable<MarketDataProtos.MarketData> marketDataObservable =
        marketDataProcessor.getMarketDataObservable();

    final TestObserver<MarketDataProtos.MarketData> testObserver =
        Observable.wrap(marketDataObservable).test();

    final List<String> symbols_101 =
        TestConstants.symbols.stream().limit(101).collect(Collectors.toList());
    final List<String> symbols_100 =
        TestConstants.symbols.stream().limit(100).collect(Collectors.toList());

    // ***********
    // Take
    // ***********
    testScheduler.advanceTimeTo(0, TimeUnit.MILLISECONDS);

    symbols_101.forEach(
        symbol -> {
          marketDataProcessor.onMessage(
              new MarketDataProcessor.NewMarketDataMsg(getMarketData(symbol, 0)));
        });

    testScheduler.advanceTimeTo(200, TimeUnit.MILLISECONDS);

    for (int i = 0, mySymbolsSize = symbols_100.size(); i < mySymbolsSize; i++) {
      String mySymbol = symbols_100.get(i);
      testObserver.assertValueAt(
          i++, q -> q.getSymbol().equals(mySymbol) && q.getLast().getNanos() == 0);
    }

    testObserver.assertValueCount(100);
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
