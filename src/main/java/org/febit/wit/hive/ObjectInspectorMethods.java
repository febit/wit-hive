/**
 * Copyright 2017-present febit.org (support@febit.org).
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
package org.febit.wit.hive;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.febit.wit.Engine;
import org.febit.wit.core.NativeFactory;
import org.febit.wit.global.GlobalManager;
import org.febit.wit.util.JavaNativeUtil;

/**
 *
 * @author zqq90
 */
public class ObjectInspectorMethods implements WitEnginePlugin {

  public static final PrimitiveObjectInspector OI_BOOLEAN = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
  public static final PrimitiveObjectInspector OI_BYTES = PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
  public static final PrimitiveObjectInspector OI_DATE = PrimitiveObjectInspectorFactory.javaDateObjectInspector;
  public static final PrimitiveObjectInspector OI_DOUBLE = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
  public static final PrimitiveObjectInspector OI_FLOAT = PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
  public static final PrimitiveObjectInspector OI_HIVE_CHAR = PrimitiveObjectInspectorFactory.javaHiveCharObjectInspector;
  public static final PrimitiveObjectInspector OI_HIVE_DECIMAL = PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector;
  public static final PrimitiveObjectInspector OI_HIVE_VARCHAR = PrimitiveObjectInspectorFactory.javaHiveVarcharObjectInspector;
  public static final PrimitiveObjectInspector OI_INT = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
  public static final PrimitiveObjectInspector OI_LONG = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
  public static final PrimitiveObjectInspector OI_SHORT = PrimitiveObjectInspectorFactory.javaShortObjectInspector;
  public static final PrimitiveObjectInspector OI_STRING = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  public static final PrimitiveObjectInspector OI_TIMESTAMP = PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
  public static final PrimitiveObjectInspector OI_VOID = PrimitiveObjectInspectorFactory.javaVoidObjectInspector;

  public static MapObjectInspector OI_MAP(ObjectInspector keyOI, ObjectInspector valueOI) {
    return ObjectInspectorFactory.getStandardMapObjectInspector(keyOI, valueOI);
  }

  public static ListObjectInspector OI_LIST(ObjectInspector valueOI) {
    return ObjectInspectorFactory.getStandardListObjectInspector(valueOI);
  }

  public static UnionObjectInspector OI_UNION(Object[] inspectors) {
    return ObjectInspectorFactory.getStandardUnionObjectInspector(toInspectorList(inspectors));
  }

  public static StandardStructObjectInspector OI_STRUCT(Object[] names, Object[] inspectors) {
    return ObjectInspectorFactory.getStandardStructObjectInspector(toStringList(names), toInspectorList(inspectors));
  }

  protected static List<String> toStringList(Object[] strings) {
    List<String> stringList = new ArrayList<>(strings.length);
    for (Object name : strings) {
      if (!(name instanceof String)) {
        throw new IllegalArgumentException("all names should be a string");
      }
      stringList.add((String) name);
    }
    return stringList;
  }

  protected static List<ObjectInspector> toInspectorList(Object[] inspectors) {
    List<ObjectInspector> inspectorList = new ArrayList<>(inspectors.length);
    for (Object inspector : inspectors) {
      if (!(inspector instanceof ObjectInspector)) {
        throw new IllegalArgumentException("all inspectors should be a ObjectInspector");
      }
      inspectorList.add((ObjectInspector) inspector);
    }
    return inspectorList;
  }

  @Override
  public void handle(Engine engine) {
    NativeFactory nativeFactory = engine.getNativeFactory();
    GlobalManager manager = engine.getGlobalManager();
    JavaNativeUtil.addStaticMethods(manager, nativeFactory, ObjectInspectorMethods.class);
    JavaNativeUtil.addConstFields(manager, nativeFactory, ObjectInspectorMethods.class);
  }
}
