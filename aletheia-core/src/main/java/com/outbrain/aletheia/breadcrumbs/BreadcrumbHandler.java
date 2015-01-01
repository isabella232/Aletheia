package com.outbrain.aletheia.breadcrumbs;

/**
 * The base interface for handling breadcrumbs emitted by the {@link BucketBasedBreadcrumbDispatcher}.
 */
public interface BreadcrumbHandler {
  void handle(final Breadcrumb breadcrumb);
}
