package com.outbrain.aletheia.breadcrumbs.persistence;

import java.util.List;

/**
 * Created by slevin on 11/16/14.
 */
public interface BreadcrumbFieldToColumnNameMapper {

  String tableName();
  String datumType();
  String source();
  String destination();
  String bucketStart();
  String bucketEnd();
  String processingTimestamp();
  String count();
  String tier();
  String application();
  String datacenter();

  List<String> allColumnNames();
}
