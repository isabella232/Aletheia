package com.outbrain.aletheia.breadcrumbs;

import java.io.Closeable;
import java.io.Serializable;

/**
 * A base interface for implementing a class that will be dispatching {@link Breadcrumb} instances.
 *
 * @param <TElement> The type of the element this {@link BreadcrumbDispatcher} will be dispatching breadcrumbs for.
 */
public interface BreadcrumbDispatcher<TElement> extends Serializable, Closeable {

  BreadcrumbDispatcher NULL = new BreadcrumbDispatcher() {
    @Override
    public void report(final Object domainClass) {

    }

    @Override
    public void close() {

    }

    @Override
    public void dispatchBreadcrumbs() {

    }
  };


  /**
   * Reports a single hit.
   *
   * @param element the item which the hit is reported on.
   */
  void report(TElement element);

  /**
   * Dispatch breadcrumbs based on the reports received so far.
   */
  void dispatchBreadcrumbs();
}
