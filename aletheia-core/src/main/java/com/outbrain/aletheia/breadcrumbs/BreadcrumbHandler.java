package com.outbrain.aletheia.breadcrumbs;

/**
 * The base interface for handling breadcrumbs emitted by the {@link BucketBasedBreadcrumbDispatcher}.
 */
public interface BreadcrumbHandler extends AutoCloseable {
  void handle(final Breadcrumb breadcrumb);
}
