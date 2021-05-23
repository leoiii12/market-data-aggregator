package info.leochoi;

import org.jetbrains.annotations.NotNull;
import org.jooq.lambda.tuple.Tuple2;

public class PriorityToSymbolTuple extends Tuple2<Long, String> {

  public PriorityToSymbolTuple(final @NotNull Long v1, final @NotNull String v2) {
    super(v1, v2);
  }

  @Override
  public int compareTo(Tuple2<Long, String> other) {
    return v1().compareTo(other.v1());
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof PriorityToSymbolTuple && v2().equals(((PriorityToSymbolTuple) o).v2());
  }

  @Override
  public int hashCode() {
    return v2().hashCode();
  }
}
