package info.leochoi;

import static info.leochoi.MarketDataProcessorConstants.MarketDataPublisher_publishAggregatedMarketData_rateLimit_count;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Bucket4j;
import io.github.bucket4j.Refill;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;
import org.jooq.lambda.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MarketDataProcessor implements Disposable {

  private static final Logger logger = LoggerFactory.getLogger(MarketDataProcessor.class);

  private final MarketDataPriorityProcessor marketDataPriorityProcessor;
  private final Subject<String> publishingSymbolSubject = PublishSubject.create();

  private final AtomicBoolean isDisposed = new AtomicBoolean(false);
  private final List<Disposable> disposables = new ArrayList<>();

  public MarketDataProcessor(final @NotNull Scheduler scheduler) {
    marketDataPriorityProcessor = new MarketDataPriorityProcessor(scheduler);

    final PriorityBlockingQueue<PriorityToSymbolTuple> queue =
        new PriorityBlockingQueue<>(
            1024, Comparator.<PriorityToSymbolTuple>comparingLong(Tuple2::v1).reversed());

    // *****
    // Source
    // *****
    marketDataPriorityProcessor.getPriorityToSymbolTupleObservable().subscribe(queue::add);

    // *****
    // Rate Limit
    // *****
    final Bucket bucket =
        Bucket4j.builder()
            .addLimit(
                Bandwidth.classic(
                    MarketDataPublisher_publishAggregatedMarketData_rateLimit_count,
                    Refill.intervally(
                        MarketDataPublisher_publishAggregatedMarketData_rateLimit_count,
                        Duration.ofSeconds(1))))
            .build();

    Observable.fromIterable(
            (Iterable<PriorityToSymbolTuple>)
                () ->
                    new Iterator<>() {
                      @Override
                      public boolean hasNext() {
                        return true;
                      }

                      @Override
                      public PriorityToSymbolTuple next() {
                        try {
                          final PriorityToSymbolTuple tuple = queue.take();
                          bucket.asScheduler().consume(1);

                          if ("A".equals(tuple.v2())) {
                            logger.info("queue.size()=[{}], tuple=[{}].", queue.size(), tuple);
                          }
                          if (tuple.v1() > 1) {
                            logger.info("tuple=[{}].", tuple);
                          }

                          return tuple;
                        } catch (InterruptedException e) {
                          throw new RuntimeException(e);
                        }
                      }
                    })
        .subscribeOn(Schedulers.newThread())
        .subscribe(
            tuple -> {
              publishingSymbolSubject.onNext(tuple.v2());
            });

    // *****
    // Publish
    // *****
    Observable.wrap(publishingSymbolSubject)
        .map(marketDataPriorityProcessor::getLatestQuote)
        .subscribe(this::publishAggregatedMarketData);
  }

  public void onMessage(final @NotNull NewMarketDataMsg newMarketDataMsg) {
    marketDataPriorityProcessor.emitNewMarketData(newMarketDataMsg.marketData);
  }

  @VisibleForTesting
  public @NotNull Observable<MarketDataProtos.MarketData> getMarketDataObservable() {
    return Observable.wrap(publishingSymbolSubject)
        .map(marketDataPriorityProcessor::getLatestQuote);
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

    if (!marketDataPriorityProcessor.isDisposed()) {
      marketDataPriorityProcessor.dispose();
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
