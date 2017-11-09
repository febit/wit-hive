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

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.febit.wit.Context;
import org.febit.wit.Engine;
import org.febit.wit.Function;
import org.febit.wit.Template;
import org.febit.wit.core.NativeFactory;
import org.febit.wit.global.GlobalManager;
import org.febit.wit.io.impl.DiscardOut;
import org.febit.wit.util.JavaNativeUtil;
import org.febit.wit.util.KeyValuesUtil;

/**
 *
 * @author zqq90
 */
public class WitUDF extends GenericUDF {

  /**
   * 传入参数的类型的变量名
   */
  public static final String KEY_PARAM_OIS = "PARAM_OIS";
  /**
   * 入口函数的变量名
   */
  public static final String KEY_MAIN = "main";
  /**
   * 返回类型的变量名
   */
  public static final String KEY_RETURN_TYPE = "TYPE";
  /**
   * 用于丢弃模板的输出
   */
  protected static final DiscardOut DISCARD_OUT = new DiscardOut();

  /**
   * 用于惰性构建模板引擎.
   */
  private static class LazyHolder {

    static final Engine ENGINE;

    static {
      ENGINE = Engine.create("febit-wit-hive-udf.wim");
      Iterator<WitEnginePlugin> iterator = ServiceLoader.load(WitEnginePlugin.class).iterator();
      while (iterator.hasNext()) {
        iterator.next().handle(ENGINE);
      }
      NativeFactory nativeFactory = ENGINE.getNativeFactory();
      GlobalManager manager = ENGINE.getGlobalManager();
      JavaNativeUtil.addStaticMethods(manager, nativeFactory, ObjectInspectorMethods.class);
      JavaNativeUtil.addConstFields(manager, nativeFactory, ObjectInspectorMethods.class);
    }
  }

  protected transient ObjectInspector[] originParamOIs;
  protected transient ObjectInspector[] paramOIs;
  protected transient Template template;
  protected transient Function witFunction;

  protected Template createTemplate(String tmpl) throws IOException {
    return LazyHolder.ENGINE.getTemplate("code:" + tmpl);
  }

  protected Context executeTemplate(Template template) throws IOException {
    return template.merge(KeyValuesUtil.wrap(KEY_PARAM_OIS, paramOIs), DISCARD_OUT);
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    // 验证第一个参数: 必须提供一个常量字符串作为模板
    if (arguments == null || arguments.length == 0) {
      throw new UDFArgumentTypeException(0, "wit template is required!");
    }
    if (!(arguments[0] instanceof ConstantObjectInspector)
      || !(arguments[0] instanceof PrimitiveObjectInspector)) {
      throw new UDFArgumentTypeException(0, "wit template should be a const string!");
    }

    // 得到模板
    String tmpl = String.valueOf(((ConstantObjectInspector) arguments[0]).getWritableConstantValue());

    // 参数类型
    originParamOIs = new ObjectInspector[arguments.length - 1];
    paramOIs = new ObjectInspector[arguments.length - 1];
    for (int i = 1; i < arguments.length; i++) {
      originParamOIs[i - 1] = arguments[i];
      paramOIs[i - 1] = ObjectInspectorUtils.getStandardObjectInspector(arguments[i], ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
    }

    // 构建模板
    try {
      template = createTemplate(tmpl);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    Context context;
    try {
      context = executeTemplate(template);
    } catch (IOException ex) {
      throw new UDFArgumentException(ex);
    }
    // 取得返回类型和入口函数
    ObjectInspector returnType = (ObjectInspector) context.get(KEY_RETURN_TYPE);
    if (returnType == null) {
      throw new UDFArgumentTypeException(0, "'TYPE' is required in wit template, like 'TYPE=OI_STRING'!");
    }
    witFunction = context.exportFunction(KEY_MAIN);
    if (witFunction == null) {
      throw new UDFArgumentTypeException(0, "'main' function is required in wit template, like 'main=()->\"hi wit\"'!");
    }
    return returnType;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object evaluate(GenericUDF.DeferredObject[] arguments) throws HiveException {
    Object[] params = new Object[paramOIs.length];
    for (int i = 1; i < arguments.length; i++) {
      // 转换参数成标准格式
      params[i - 1] = copyToStandardObject(arguments[i].get(), originParamOIs[i - 1]);
    }
    // 此处直接返回了模板的返回值, 没有进行额外的类型转换
    // 因此要注意, 类型必须要符合 TYPE 中定义的那样
    //   基本类型为: Java基本类型
    //   数组为: 数组或者 List
    //   Map 为: Map
    //   Struct 为: 数组或者 List
    return witFunction.invoke(params);
  }

  /**
   * 转换参数.
   *
   * refer: org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils
   *
   * @param o
   * @param oi
   * @return
   */
  public static Object copyToStandardObject(Object o, ObjectInspector oi) {
    if (o == null) {
      return null;
    }
    switch (oi.getCategory()) {
      case PRIMITIVE: {
        PrimitiveObjectInspector loi = (PrimitiveObjectInspector) oi;
        Object result = loi.getPrimitiveJavaObject(o);
        if (result instanceof Timestamp) {
          // Timestamp 是可变的, 需要复制一份
          Timestamp source = (Timestamp) result;
          Timestamp copy = new Timestamp(source.getTime());
          copy.setNanos(source.getNanos());
          return copy;
        }
        return result;
      }
      case LIST: {
        ListObjectInspector loi = (ListObjectInspector) oi;
        ObjectInspector elementOI = loi.getListElementObjectInspector();
        int length = loi.getListLength(o);
        ArrayList<Object> list = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
          list.add(copyToStandardObject(loi.getListElement(o, i), elementOI));
        }
        return list;
      }
      case MAP: {
        MapObjectInspector moi = (MapObjectInspector) oi;
        ObjectInspector keyOI = moi.getMapKeyObjectInspector();
        ObjectInspector valueOI = moi.getMapValueObjectInspector();
        HashMap<Object, Object> map = new HashMap<>();
        for (Map.Entry<?, ?> entry : moi.getMap(o).entrySet()) {
          map.put(copyToStandardObject(entry.getKey(), keyOI),
            copyToStandardObject(entry.getValue(), valueOI));
        }
        return map;
      }
      case STRUCT: {
        StructObjectInspector soi = (StructObjectInspector) oi;
        StructList struct = new StructList(soi);
        for (StructField f : soi.getAllStructFieldRefs()) {
          struct.add(copyToStandardObject(soi.getStructFieldData(o, f), f.getFieldObjectInspector()));
        }
        return struct;
      }
      case UNION: {
        UnionObjectInspector uoi = (UnionObjectInspector) oi;
        List<ObjectInspector> objectInspectors = uoi.getObjectInspectors();
        return copyToStandardObject(uoi.getField(o),
          objectInspectors.get(uoi.getTag(o)));
      }
      default: {
        throw new RuntimeException("Unknown ObjectInspector category!");
      }
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("wit", children);
  }
}
