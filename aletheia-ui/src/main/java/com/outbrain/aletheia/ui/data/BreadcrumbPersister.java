package com.outbrain.aletheia.breadcrumbs;

/**
 * Created by slevin on 10/2/14.
 */
public interface BreadcrumbPersister {

  public static BreadcrumbPersister NULL = new BreadcrumbPersister() {
    @Override
    public void persist(final Breadcrumb breadcrumb) {

    }
  };

  void persist(Breadcrumb breadcrumb);
}
