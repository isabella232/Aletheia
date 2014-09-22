package com.outbrain.aletheia.metrics.common;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;

import java.util.concurrent.atomic.AtomicLong;

public class SafeRatioHolder implements RatioHolder {
  AtomicLong averageContainer = new AtomicLong();
  private final int numberOfBitsForNominator;
  private final long maxDenom;
  private final long maxNom;

  public SafeRatioHolder(final int expectedNominator, final int expectedDenominator) {

    Preconditions.checkArgument(expectedDenominator <= expectedNominator,
                                "expected denominator should be smaller or equal the expected nominator");

    final int bitsNeededForRatio = Integer.numberOfTrailingZeros(Integer.highestOneBit(expectedNominator / expectedDenominator));

    final int numberOfBitForDenomenator = (int) Math.ceil((63 - bitsNeededForRatio) / 2.0);
    numberOfBitsForNominator = 63 - numberOfBitForDenomenator;

    maxDenom = Long.parseLong("0" + StringUtils.repeat("1", 63 - numberOfBitsForNominator) + StringUtils.repeat("0",
                                                                                                                numberOfBitsForNominator),
                              2);
    maxNom = Long.parseLong(StringUtils.repeat("0", 64 - numberOfBitsForNominator) + StringUtils.repeat("1",
                                                                                                        numberOfBitsForNominator),
                            2);
  }

  @Override
  public void addDeltas(final int nomDelta, final int denomDelta) {
    final long shiftedDenomDelta = ((long) denomDelta) << numberOfBitsForNominator;
    final long increase = nomDelta + shiftedDenomDelta;
    averageContainer.getAndAdd(increase);
  }

  @Override
  public Double resetAndReturnLastValue() {
    final long val = averageContainer.get();
    final long denominator = ((val & maxDenom) >> numberOfBitsForNominator);
    if (denominator == 0) {
      return null;
    }
    final long nom = val & maxNom;
    averageContainer.set(0l);

    return (nom / (double) denominator);
  }
}