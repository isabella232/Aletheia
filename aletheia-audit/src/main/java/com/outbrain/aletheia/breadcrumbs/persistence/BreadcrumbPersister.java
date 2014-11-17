package com.outbrain.aletheia.breadcrumbs.persistence;

import com.outbrain.aletheia.breadcrumbs.Breadcrumb;

import java.util.List;

/**
 * Created by slevin on 10/2/14.
 */
public interface BreadcrumbPersister {

  public static BreadcrumbPersister NULL = new BreadcrumbPersister() {
    @Override
    public int persist(final List<Breadcrumb> breadcrumbs) {
      return breadcrumbs.size();
    }
  };

  int persist(final List<Breadcrumb> breadcrumbs);
}
