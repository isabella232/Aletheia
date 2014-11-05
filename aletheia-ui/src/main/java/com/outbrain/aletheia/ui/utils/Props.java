/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.outbrain.aletheia.ui.utils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Props {

  private final Properties props;

  public Props(final File f) throws IOException {
    this.props = new Properties();
    this.props.load(new FileReader(f));
  }

  public Props(final Properties p) {
    this.props = p;
  }

  public String getString(final String name) {
    final String s = this.props.getProperty(name);
    if (s == null)
      throw new IllegalArgumentException("Missing required property '" + name + "'");
    else
      return s.trim();
  }

  public String getString(final String name, final String otherwise) {
    if (this.props.containsKey(name))
      return this.props.getProperty(name).trim();
    else
      return otherwise;
  }

  public int getInt(final String name) {
    return Integer.parseInt(getString(name));
  }

  public int getInt(final String name, final int otherwise) {
    if (has(name))
      return Integer.parseInt(getString(name));
    else
      return otherwise;
  }

  public int getLong(final String name) {
    return Integer.parseInt(getString(name));
  }

  public long getLong(final String name, final long otherwise) {
    if (has(name))
      return Long.parseLong(getString(name));
    else
      return otherwise;
  }

  public int getDouble(final String name) {
    return Integer.parseInt(getString(name));
  }

  public double getDouble(final String name, final double otherwise) {
    if (has(name))
      return Double.parseDouble(getString(name));
    else
      return otherwise;
  }

  public boolean getBoolean(final String name) {
    if ("true".equals(getString(name)))
      return true;
    else if ("false".equals(getString(name)))
      return false;
    else
      throw new IllegalArgumentException(name + " is not a valid boolean value.");
  }

  public boolean getBoolean(final String name, final boolean otherwise) {
    if (has(name))
      return getBoolean(name);
    else
      return otherwise;
  }

  public List<String> getStringList(final String key, final List<String> otherwise) {
    return getStringList(key, "\\s*,\\s*", otherwise);
  }

  public List<String> getStringList(final String key) {
    if (has(key))
      return getStringList(key, null);
    else
      throw new IllegalArgumentException("Missing required property '" + key + "'");
  }

  public List<String> getStringList(final String key, final String sep, final List<String> otherwise) {
    if (!has(key))
      return Collections.emptyList();

    final String val = getString(key);
    if (val.trim().length() == 0)
      return Collections.emptyList();

    if (has(key))
      return Arrays.asList(val.split(sep));
    else
      return otherwise;
  }

  public Set<String> keys() {
    final Set<String> keys = new HashSet<String>();
    for (final Object k : this.props.keySet())
      keys.add((String) k);
    return keys;
  }

  public boolean has(final String name) {
    final String s = this.props.getProperty(name);
    return s != null && s.trim().length() > 0;
  }

  public void set(final String key, final String val) {
    this.props.setProperty(key, val);
  }

  public Properties getProperties() {
    return this.props;
  }
}
