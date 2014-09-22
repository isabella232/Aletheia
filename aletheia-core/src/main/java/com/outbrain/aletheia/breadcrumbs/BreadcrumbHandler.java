package com.outbrain.aletheia.breadcrumbs;

/**
 * The base interface for handling breadcrumbs emitted by the <code>BucketBasedBreadcrumbDispatcher</code>.
 */
public interface BreadcrumbHandler {
  void handle(final Breadcrumb breadcrumb);
}
