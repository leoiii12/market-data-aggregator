package info.leochoi;

import static info.leochoi.MarketDataProcessorConstants.MarketDataCoreProcessor_publishAggregatedMarketData_throttle_ms;

import info.leochoi.MarketDataProtos.MarketData;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;
import org.jooq.lambda.tuple.Tuple2;

/**
 * This class throttles the source {@link MarketDataProtos.MarketData} events and provides
 * observables methods for getting the results in a continuous manner. Priority (Count in the
 * second) is also zipped with the result.
 *
 * <p>Higher Frequency --> Higher Priority
 */
public class MarketDataPriorityProcessor implements Disposable {

  private final ConcurrentHashMap<String, MarketDataProtos.MarketData> symbolToMarketDataMap =
      new ConcurrentHashMap<>();

  private final Subject<MarketDataProtos.MarketData> marketDataSubject = PublishSubject.create();
  private final Subject<PriorityToSymbolTuple> priorityToSymbolSubject = PublishSubject.create();

  private final AtomicBoolean isDisposed = new AtomicBoolean(false);
  private final List<Disposable> disposables = new ArrayList<>();

  public MarketDataPriorityProcessor(final @NonNull Scheduler scheduler) {
    // 1. Ensure each symbol will always have the latest market data when it is published
    disposables.add(
        Observable.wrap(marketDataSubject)
            .subscribe(
                marketData -> {
                  symbolToMarketDataMap.put(marketData.getSymbol(), marketData);
                }));

    // 2. Ensure each symbol will not have more than one update per second
    // However, if the symbol has more than one changes in the second, throttleLatest emits the last
    // changes in the second at the beginning of the another second
    //
    // Here also counts how many raw records are emitted in the corresponding window
    // Tuple2<CountInTheSecond, MarketData>
    disposables.add(
        Observable.wrap(marketDataSubject)
            .groupBy(MarketDataProtos.MarketData::getSymbol)
            .subscribe(
                g -> {
                  // Here is single-threaded. But AtomicLong is still being used now for latter easy
                  // migrations to the multi-threaded env.
                  final AtomicLong priority = new AtomicLong(1);

                  // Adds the priority
                  final Observable<Tuple2<Long, MarketData>> tuple2Observable =
                      g.map(md -> new Tuple2<>(priority.getAndIncrement(), md)).share();

                  // Emits the tuple
                  final Disposable disposable1 =
                      tuple2Observable
                          .throttleLatest(
                              MarketDataCoreProcessor_publishAggregatedMarketData_throttle_ms,
                              TimeUnit.MILLISECONDS,
                              scheduler,
                              true)
                          .subscribe(
                              t ->
                                  priorityToSymbolSubject.onNext(
                                      new PriorityToSymbolTuple(t.v1(), t.v2().getSymbol())));

                  // Resets the priority after the second ends
                  final Disposable disposable2 =
                      tuple2Observable
                          .throttleLast(
                              MarketDataCoreProcessor_publishAggregatedMarketData_throttle_ms,
                              TimeUnit.MILLISECONDS,
                              scheduler)
                          .subscribe(tuple2 -> priority.set(1));

                  disposables.add(disposable1);
                  disposables.add(disposable2);
                }));
  }

  @VisibleForTesting
  public @NotNull Observable<MarketDataProtos.MarketData> getMarketDataObservable() {
    return Observable.wrap(marketDataSubject);
  }

  public @NotNull Observable<PriorityToSymbolTuple> getPriorityToSymbolTupleObservable() {
    return Observable.wrap(priorityToSymbolSubject);
  }

  public void emitNewMarketData(@NonNull MarketDataProtos.MarketData quote) {
    marketDataSubject.onNext(quote);
  }

  /**
   * This checks whether the symbol has been processed by this class {@link
   * info.leochoi.MarketDataPriorityProcessor}. If the invoker is not using {@link
   * MarketDataPriorityProcessor#getLatestMarketData(java.lang.String)}, then this method should be
   * invoked first.
   *
   * @param symbol
   * @return true --> processed, false --> never processed
   */
  public boolean containsSymbol(@NotNull String symbol) {
    return symbolToMarketDataMap.containsKey(symbol);
  }

  /**
   * This gets the latest {@link MarketDataProtos.MarketData} processed by this class {@link
   * info.leochoi.MarketDataPriorityProcessor}.
   *
   * @param symbol
   * @return the latest {@link MarketDataProtos.MarketData}
   */
  public @NotNull MarketDataProtos.MarketData getLatestMarketData(@NotNull String symbol) {
    if (!containsSymbol(symbol)) {
      throw new UnknownSymbolException(symbol);
    }

    return symbolToMarketDataMap.get(symbol);
  }

  @Override
  public void dispose() {
    final boolean isDisposed = this.isDisposed.getAndSet(true);
    if (isDisposed) {
      return;
    }

    disposables.forEach(
        d -> {
          if (d.isDisposed()) {
            return;
          }

          d.dispose();
        });
  }

  @Override
  public boolean isDisposed() {
    return isDisposed.get();
  }

  public static class UnknownSymbolException extends RuntimeException {
    public final String symbol;

    public UnknownSymbolException(String symbol) {
      this.symbol = symbol;
    }
  }
}
