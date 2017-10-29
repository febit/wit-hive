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

import org.febit.wit.hive.StructList;
import org.febit.wit.resolvers.GetResolver;
import org.febit.wit.resolvers.SetResolver;

/**
 *
 * @author zqq90
 */
public class StructListResolver implements GetResolver, SetResolver {

  @Override
  public Object get(Object object, Object property) {
    if (property instanceof Number) {
      return ((StructList) object).get(((Number) property).intValue());
    } else {
      return ((StructList) object).getByName(property);
    }
  }

  @Override
  public void set(Object object, Object property, Object value) {
    if (property instanceof Number) {
      ((StructList) object).set(((Number) property).intValue(), value);
    } else {
      ((StructList) object).setByName(property, value);
    }
  }

  @Override
  public Class getMatchClass() {
    return StructList.class;
  }

}
