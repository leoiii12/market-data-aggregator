package info.leochoi;

import static info.leochoi.MarketDataProcessorConstants.MarketDataPublisher_publishAggregatedMarketData_rateLimit_count;
import static info.leochoi.MarketDataProcessorConstants.MarketDataPublisher_publishAggregatedMarketData_rateLimit_ms;

import info.leochoi.MarketDataProtos.MarketData;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;
import org.jooq.lambda.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MarketDataProcessor implements Disposable {

  private static final Logger logger = LoggerFactory.getLogger(MarketDataProcessor.class);

  private final MarketDataCoreProcessor marketDataCoreProcessor;
  private final Subject<MarketData> marketDataSubject = PublishSubject.create();

  private final AtomicBoolean isDisposed = new AtomicBoolean(false);
  private final List<Disposable> disposables = new ArrayList<>();

  public MarketDataProcessor(final @NotNull Scheduler scheduler) {
    marketDataCoreProcessor = new MarketDataCoreProcessor(scheduler);

    final Disposable takeDisposable =
        marketDataCoreProcessor
            .getPriorityToMarketDataTuple2Observable()
            .window(MarketDataPublisher_publishAggregatedMarketData_rateLimit_ms, TimeUnit.MILLISECONDS, scheduler)
            .subscribe(
                tuple2Observable -> {
                  final Observable<Tuple2<Long, MarketData>> observable =
                      tuple2Observable.replay().autoConnect();

                  observable
                      .take(MarketDataPublisher_publishAggregatedMarketData_rateLimit_count)
                      .subscribe(
                          tuple2 -> {
                            marketDataSubject.onNext(tuple2.v2());
                          });

                  observable
                      .skip(MarketDataPublisher_publishAggregatedMarketData_rateLimit_count)
                      .subscribe(
                          tuple2 -> {
                            logger.warn("Unhandled. tuple2=[{}].", tuple2);
                          });
                });

    final Disposable publishAggregatedMarketDataDisposable =
        Observable.wrap(marketDataSubject).subscribe(this::publishAggregatedMarketData);

    disposables.add(takeDisposable);
    disposables.add(publishAggregatedMarketDataDisposable);
  }

  public void onMessage(final @NotNull NewMarketDataMsg newMarketDataMsg) {
    marketDataCoreProcessor.emitNewMarketData(newMarketDataMsg.marketData);
  }

  @VisibleForTesting
  public @NotNull Observable<MarketDataProtos.MarketData> getMarketDataObservable() {
    return Observable.wrap(marketDataSubject);
  }

  private void publishAggregatedMarketData(final @NotNull MarketDataProtos.MarketData marketData) {
    // TODO
     System.out.println(marketData);
  }

  @Override
  public void dispose() {
    final boolean isDisposed = this.isDisposed.getAndSet(true);
    if (isDisposed) {
      return;
    }

    if (!marketDataCoreProcessor.isDisposed()) {
      marketDataCoreProcessor.dispose();
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

  public static class NewMarketDataMsg {
    private final MarketDataProtos.MarketData marketData;

    public NewMarketDataMsg(MarketDataProtos.MarketData quote) {
      this.marketData = quote;
    }
  }
}
