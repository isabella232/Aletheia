package com.outbrain.aletheia.breadcrumbs.persistence;

import com.google.common.collect.Lists;
import com.outbrain.aletheia.Props;

import java.util.List;

/**
 * Created by slevin on 11/16/14.
 */
public class PropsBreadcrumbFieldToColumnNameMapper implements BreadcrumbFieldToColumnNameMapper {

  private final Props props;

  public PropsBreadcrumbFieldToColumnNameMapper(Props props) {
    this.props = props;
  }

  @Override
  public String tableName() {
    return props.getString("breadcrumb.table.name", "dabd_breadcrumb_data");
  }

  @Override
  public String datumType() {
    return props.getString("breadcrumb.table.tier.column.name", "dabd_hit_type");
  }

  @Override
  public String source() {
    return props.getString("breadcrumb.table.source.column.name", "dabd_source");
  }

  @Override
  public String destination() {
    return props.getString("breadcrumb.table.destination.column.name", "dabd_destination");
  }

  @Override
  public String bucketStart() {
    return props.getString("breadcrumb.table.bucket_start.column.name", "dabd_bucket_start_time");
  }

  @Override
  public String bucketEnd() {
    return props.getString("breadcrumb.table.bucket_end.column.name", "dabd_bucket_end_time");
  }

  @Override
  public String processingTimestamp() {
    return props.getString("breadcrumb.table.processing_time.column.name", "dabd_bucket_processing_timestamp");
  }

  @Override
  public String count() {
    return props.getString("breadcrumb.table.count.column.name", "dabd_bucket_hit_count");
  }

  @Override
  public String tier() {
    return props.getString("breadcrumb.table.tier.column.name", "dabd_tier");
  }

  @Override
  public String application() {
    return props.getString("breadcrumb.table.application.column.name", "dabd_application");
  }

  @Override
  public String datacenter() {
    return props.getString("breadcrumb.table.datacenter.column.name", "dabd_data_center");
  }

  @Override
  public List<String> allColumnNames() {
    return Lists.newArrayList(datumType(),
                              source(),
                              destination(),
                              bucketStart(),
                              bucketEnd(),
                              processingTimestamp(),
                              count(),
                              tier(),
                              application(),
                              datacenter());
  }
}
