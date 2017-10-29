# wit-hive

提供一个可用于在 hive sql 中执行脚本的 UDF (自定义函数)


## Show off ;)

```sql
-- Tip: TYPE 和 main 变量是免声明的, 其他变量需要声明
-- Tip: 使用 Lambda 表达式通常会比 function(..){..} 更简洁

SELECT 
  wit('TYPE=OI_STRING; main=()->"Hi WIT"'),
  wit('TYPE=OI_STRUCT(["id", "name"], [OI_INT, OI_STRING]); main=()->[9527, "Mr. Wit"]'),
  -- 获取 List 的最后一个元素
  wit('TYPE=PARAM_OIS[0].elementOI; main=(list)-> list.size > 0 && list[list.size-1] || null', array()),
  -- 获取 Map 的 values
  wit('TYPE=OI_LIST(PARAM_OIS[0].valueOI); main=(map)->map.~values().~toArray()', map("A",array("a","A"),"B",array("b","B"))),
  -- 获取 Struct 的 name 字段
  wit('TYPE=PARAM_OIS[0].name.oi; main=(bean)->bean.name', named_struct("id", 9527, "name", "Mr. Wit")),
  -- 获取 Struct 的 name 字段 (推荐: 提前获取 id, 即数组索引, 并使用索引获取值)
  wit('TYPE=PARAM_OIS[0].name.oi; var index=PARAM_OIS[0].name.id;  main=(bean)->bean[index]', named_struct("id", 9527, "name", "Mr. Wit"))
  ;

WITH t AS(
  -- 我们用 concat 来实现多行, 方便阅读, 这里实际上会被优化成常量字符串传入 wit, 因此不用担心会报错
  SELECT inline(wit(concat(
  'TYPE=OI_LIST(OI_STRUCT(["id","name","scores"], [OI_INT,OI_STRING,OI_MAP(OI_STRING, OI_INT)]));',
  'var genScore=()->org.apache.commons.lang.math.RandomUtils::nextInt(70)+30;',
  'var genName=()->org.apache.commons.lang3.RandomStringUtils::randomAlphabetic(6);',
  'main=()-> {',
  '  var list = java.util.ArrayList::new();',
  '  for(id : 1001 .. 1004) {',
  -- 注意: Struct 需要数组/List, 字段按照声明时的顺序
  '    list.~add([id, genName(), { CourseX: genScore(), "Course A": genScore(), "Course B": genScore() }]);',
  '  }',
  '  return list;',
  '};'
  )))
)
SELECT 
  id, course, score,
  wit('TYPE=OI_STRING; main=(s)-> s>=90 ? "A" : s>=75 ? "B" : s>=60 ? "C" : "D" ', score) AS Grade,
  wit('TYPE=OI_LIST(OI_INT); main=(n)->[n-1,n+1]', id) AS neighbor, 
  wit('TYPE=OI_STRING; main=()->java.util.UUID::randomUUID()', id) AS UUID, -- 需要传入一个非常量, 否则 Hive 可能会对结果进行优化, 
  wit('TYPE=OI_STRING; main=()->java.util.UUID::randomUUID()') AS BAD_UUID, -- 导致输出相同的值
  wit('TYPE=OI_STRING; var i = 0; main = () -> i++', id) AS seq,  -- 延时公共变量, 但是非线程安全! 跨界点或者多线程无法保证递增
  0
FROM t
  LATERAL VIEW explode(scores) ex_scores AS course, score;
```

## build

```sh
mvn package -Pdist
```

## Usage

+ 将 target 目录下的 `wit-hive-<version>-dist.jar` 上传到 HDFS

> 也可以是其他 Hive 自定义 UDF 支持的 URI

+ 注册函数

```sql
CREATE FUNCTION wit AS 'org.febit.wit.hive.WitUDF' USING JAR 'hdfs://<host>:<port>/path/to/wit-hive-0.1.0-SNAPSHOT-dist.jar';
```

或者是临时函数

```sql
CREATE TEMPORARY FUNCTION wit AS 'org.febit.wit.hive.WitUDF' USING JAR 'hdfs://<host>:<port>/path/to/wit-hive-0.1.0-SNAPSHOT-dist.jar';
```

+ 验证

```sql
SELECT wit('TYPE=OI_STRING; main=()->"Hi WIT"');
```

## 其他

+ 在 `hive` 命令行中 `;` 可能需要被转义, 例如: `SELECT wit('TYPE=OI_STRING\; main=()->"Hi WIT"');`
+ 更多请关注 https://wit.febit.net/ 和 https://github.com/febit/wit
