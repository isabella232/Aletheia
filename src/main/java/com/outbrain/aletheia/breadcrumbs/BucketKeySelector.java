package com.outbrain.aletheia.breadcrumbs;

public interface BucketKeySelector<TElement, TBucketKey> {

    TBucketKey selectKey(TElement item);
}
