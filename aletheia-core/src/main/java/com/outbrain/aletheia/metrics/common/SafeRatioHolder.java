package com.outbrain.aletheia.metrics.common;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.atomic.AtomicLong;

public class SafeRatioHolder implements RatioHolder {
  AtomicLong averageContainer = new AtomicLong();
  private final int numberOfBitsForNominator;
  private final long maxDenominator;
  private final long maxNominator;

  public SafeRatioHolder(final int expectedNominator, final int expectedDenominator) {

    Preconditions.checkArgument(expectedDenominator <= expectedNominator,
                                "expected denominator should be smaller or equal the expected nominator");

    final int bitsNeededForRatio =
            Integer.numberOfTrailingZeros(Integer.highestOneBit(expectedNominator / expectedDenominator));

    final int numberOfBitForDenominator = (int) Math.ceil((63 - bitsNeededForRatio) / 2.0);
    numberOfBitsForNominator = 63 - numberOfBitForDenominator;

    maxDenominator = Long.parseLong("0" + StringUtils.repeat("1", 63 - numberOfBitsForNominator) + StringUtils.repeat("0",
                                                                                                                numberOfBitsForNominator),
                              2);
    maxNominator = Long.parseLong(StringUtils.repeat("0", 64 - numberOfBitsForNominator) + StringUtils.repeat("1",
                                                                                                        numberOfBitsForNominator),
                            2);
  }

  @Override
  public void addDeltas(final int nomDelta, final int denominatorDelta) {
    final long shiftedDenominatorDelta = ((long) denominatorDelta) << numberOfBitsForNominator;
    final long increase = nomDelta + shiftedDenominatorDelta;
    averageContainer.getAndAdd(increase);
  }

  @Override
  public Double resetAndReturnLastValue() {
    final long val = averageContainer.get();
    final long denominator = ((val & maxDenominator) >> numberOfBitsForNominator);
    if (denominator == 0) {
      return null;
    }
    final long nom = val & maxNominator;
    averageContainer.set(0l);

    return (nom / (double) denominator);
  }
}