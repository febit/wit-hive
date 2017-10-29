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
package org.febit.wit.hive.resolver;

import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.febit.wit.exceptions.ScriptRuntimeException;
import org.febit.wit.resolvers.GetResolver;

/**
 *
 * @author zqq90
 */
public class StructFieldResolver implements GetResolver {

  @Override
  public Object get(Object object, Object property) {
    StructField field = (StructField) object;
    switch (String.valueOf(property)) {
      case "id":
      case "index":
        return field.getFieldID();
      case "name":
        return field.getFieldName();
      case "comment":
        return field.getFieldComment();
      case "OI":
      case "oi":
      case "objectInspector":
        return field.getFieldObjectInspector();
      default:
        throw new ScriptRuntimeException("Invalid property or can't read: org.apache.hadoop.hive.serde2.objectinspector.StructField#" + property);
    }
  }

  @Override
  public Class getMatchClass() {
    return StructField.class;
  }

}
