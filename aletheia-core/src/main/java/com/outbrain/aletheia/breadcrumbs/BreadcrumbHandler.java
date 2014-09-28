package com.outbrain.aletheia.breadcrumbs;

/**
 * The base interface for handling breadcrumbs emitted by the {@code BucketBasedBreadcrumbDispatcher}.
 */
public interface BreadcrumbHandler {
  void handle(final Breadcrumb breadcrumb);
}
