package com.outbrain.aletheia.configuration;

import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

import static org.hamcrest.core.Is.is;

/**
 * Created by slevin on 6/16/15.
 */
public class PropertyUtilsTest {
  @Test
  public void test_whenOverridingPropertiesWithKeepAll_oldValuesGetOverriddenNewOnesAreCopied() throws Exception {

    final Properties a = new Properties();
    a.setProperty("a", "1");
    a.setProperty("b", "2");

    final Properties b = new Properties();
    b.setProperty("a", "11");
    b.setProperty("b", "22");

    b.setProperty("c", "33");

    final Properties result = PropertyUtils.override(a).with(b).all();

    Assert.assertThat(result.size(), is(3));
    Assert.assertThat(result.getProperty("a"), is("11"));
    Assert.assertThat(result.getProperty("b"), is("22"));
    Assert.assertThat(result.getProperty("c"), is("33"));
  }

  @Test
  public void test_whenOverridingPropertiesWithIntersectedOnly_oldValuesGetOverriddenNewOnesAreDiscarded()
          throws Exception {
    final Properties a = new Properties();
    a.setProperty("a", "1");
    a.setProperty("b", "2");

    final Properties b = new Properties();
    b.setProperty("a", "11");
    b.setProperty("b", "22");

    b.setProperty("c", "33");

    final Properties result = PropertyUtils.override(a).with(b).intersectingOnly();

    Assert.assertThat(result.size(), is(2));
    Assert.assertThat(result.getProperty("a"), is("11"));
    Assert.assertThat(result.getProperty("b"), is("22"));
  }

  @Test
  public void test_whenOverridingWithEmptyProperties_oldValuesRetained() throws Exception {

    final Properties a = new Properties();
    a.setProperty("a", "1");
    a.setProperty("b", "2");

    final Properties b = new Properties();

    final Properties all = PropertyUtils.override(a).with(b).all();
    final Properties intersected = PropertyUtils.override(a).with(b).intersectingOnly();

    Assert.assertThat(all.size(), is(2));
    Assert.assertThat(all.getProperty("a"), is("1"));
    Assert.assertThat(all.getProperty("b"), is("2"));

    Assert.assertThat(intersected.size(), is(2));
    Assert.assertThat(intersected.getProperty("a"), is("1"));
    Assert.assertThat(intersected.getProperty("b"), is("2"));
  }

  @Test
  public void test_whenOverridingEmptyAndKeepingAll_newValuesAreSet() throws Exception {

    final Properties a = new Properties();

    final Properties b = new Properties();
    b.setProperty("a", "1");
    b.setProperty("b", "2");

    final Properties result = PropertyUtils.override(a).with(b).all();

    Assert.assertThat(result.size(), is(2));
    Assert.assertThat(result.getProperty("a"), is("1"));
    Assert.assertThat(result.getProperty("b"), is("2"));
  }

  @Test
  public void test_whenOverridingEmptyAndKeepingIntersected_noValuesAreSet() throws Exception {

    final Properties a = new Properties();

    final Properties b = new Properties();
    b.setProperty("a", "1");
    b.setProperty("b", "2");

    final Properties result = PropertyUtils.override(a).with(b).intersectingOnly();

    Assert.assertThat(result.size(), is(0));
  }
}