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
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 *
 * @author zqq90
 */
public class StructList extends ArrayList<Object> {

  protected final StructObjectInspector soi;

  public StructList(StructObjectInspector soi) {
    super(soi.getAllStructFieldRefs().size());
    this.soi = soi;
  }

  protected int getFieldIndex(Object name) {
    return soi.getStructFieldRef(name.toString()).getFieldID();
  }

  public void setByName(Object name, Object value) {
    set(getFieldIndex(name), value);
  }

  public Object getByName(Object name) {
    return get(getFieldIndex(name));
  }

}
