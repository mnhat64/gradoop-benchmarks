/*
 * Copyright © 2014 - 2024 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.benchmarks.utils;

/**
 * An enum to represent the supported gradoop output formats.
 */
public enum GradoopFormat {
  CSV("csv"),
  INDEXED_CSV("indexed"),
  PARQUET("parquet"),
  PARQUET_PROTOBUF("protobuf");

  /**
   * A String representation of the enum, used for program arguments.
   */
  public final String name;

  /**
   * Creates an instance of this enum.
   *
   * @param name the String representation
   */
  private GradoopFormat(String name) {
    this.name = name;
  }

  /**
   * Returns the enum instance by the given String representation or {@code null} otherwise.
   *
   * @param name the String representation of the enum (csv, indexed, parquet or protobuf)
   * @return the respective enum
   */
  public static GradoopFormat getByName(String name) {
    for (GradoopFormat format : values()) {
      if (format.name.equals(name)) {
        return format;
      }
    }
    return null;
  }
}
