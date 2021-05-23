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

public class MarketDataPriorityProcessor implements Disposable {

  private final ConcurrentHashMap<String, MarketDataProtos.MarketData> symbolToMarketDataMap =
      new ConcurrentHashMap<>();

  private final Subject<MarketDataProtos.MarketData> marketDataSubject = PublishSubject.create();
  private final Subject<PriorityToSymbolTuple> priorityToSymbolSubject = PublishSubject.create();

  private final AtomicBoolean isDisposed = new AtomicBoolean(false);
  private final List<Disposable> disposables = new ArrayList<>();

  public MarketDataPriorityProcessor(final @NonNull Scheduler scheduler) {
    // Ensure each symbol will always have the latest market data when it is published
    disposables.add(
        Observable.wrap(marketDataSubject)
            .subscribe(
                marketData -> {
                  symbolToMarketDataMap.put(marketData.getSymbol(), marketData);
                }));

    // Ensure each symbol will not have more than one update per second
    // Here also counts how many raw records are emitted in the corresponding window
    // Tuple2<CountInTheWindow, MarketData>
    disposables.add(
        Observable.wrap(marketDataSubject)
            .groupBy(MarketDataProtos.MarketData::getSymbol)
            .subscribe(
                g -> {
                  final AtomicLong atomicLong = new AtomicLong(1);

                  // Adds the index
                  final Observable<Tuple2<Long, MarketData>> tuple2Observable =
                      g.map(md -> new Tuple2<>(atomicLong.getAndIncrement(), md)).share();

                  // Emits the value
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

                  // Resets the priority after the window
                  final Disposable disposable2 =
                      tuple2Observable
                          .throttleLast(
                              MarketDataCoreProcessor_publishAggregatedMarketData_throttle_ms,
                              TimeUnit.MILLISECONDS,
                              scheduler)
                          .subscribe(tuple2 -> atomicLong.set(1));

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

  public boolean containsSymbol(@NotNull String symbol) {
    return symbolToMarketDataMap.containsKey(symbol);
  }

  public @NotNull MarketDataProtos.MarketData getLatestQuote(@NotNull String symbol) {
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
