package com.outbrain.aletheia.datum.consumption;

/**
 * Created by irolnik on 2/13/17.
 */
public enum OffsetCommitMode {
  // Offset of consumed records will be committed automatically with frequency controlled by the config auto.commit.interval.ms
  AT_LEAST_ONCE,
  // Offset will be committed immedaitelly after poll
  AT_MOST_ONCE,
  // Offset will be controlled by the user using the DatumEnvelopeFetcher.commitConsumedOffsets method
  MANUAL
}
