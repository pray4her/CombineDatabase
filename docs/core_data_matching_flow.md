# 当前项目数据匹配流程说明

## 1. 文档目的
本文基于当前仓库 `core/` 目录下的实际代码实现，对项目现有的数据清洗与匹配流程做一次“按代码落地”的梳理，重点回答以下问题：

- 当前代码如何生成作者记录；
- 如何把作者名称和邮箱进行匹配；
- 当前代码在哪些地方做了去重，哪些地方没有做；
- `core/` 下每个脚本在整条链路中的职责是什么。

本文只描述“当前代码实际在做什么”，不展开需求假设，也不按理想设计重写流程。

## 2. `core/` 目录脚本职责总览

### `core/__init__.py`
- 仅用于声明核心包，没有业务逻辑。

### `core/concurrency.py`
- 负责整个 ETL 多进程拓扑。
- 主流程是：扫描输入文件 -> 启动多个 Worker -> 启动单独 Writer -> 汇总进度。
- 还负责 SQLite 表结构初始化，以及 QS200 / Fortune500 参考数据的预加载。

### `core/etl_worker.py`
- 负责“单个文件、逐行处理”。
- 会读取 Excel/CSV 行数据，抽取 `Reprint Addresses`、`Email Addresses`、`Addresses`、`Author Full Names` 等字段。
- 调用 `core.parsing` 完成作者解析、作者-邮箱对齐、全名补全，再拼成最终待入库记录。

### `core/parsing.py`
- 是作者生成和作者-邮箱匹配的核心模块。
- 负责：
  - 从 `Reprint Addresses` 中提取作者简称；
  - 从 `Addresses` 中提取作者全名、国家、机构；
  - 计算“作者名 vs 邮箱前缀”的相似度；
  - 在单邮箱/多邮箱场景下做对齐；
  - 在局部场景下做作者去重；
  - 在缺少全名时，用 `Author Full Names` 回填全名。

### `core/exporter.py`
- 负责从 SQLite 导出 Excel。
- 同时包含若干“后处理能力”：
  - 华人/海外华人/外国人分类；
  - 普通去重；
  - 按邮箱+相似度的精细化去重；
  - 低相似度记录抽出；
  - QS200 / Fortune500 命中增强。
- 注意：这些能力多数不是主 ETL 入库时自动发生的，而是导出或后处理阶段才调用。

### `core/email_validation.py`
- 负责邮箱有效性验证。
- 验证分三层：语法校验 -> DNS/MX -> SMTP。
- 这是数据库入库后的后处理能力，不参与“作者是谁、邮箱对应谁”的主匹配决策。

### `core/qs200.py`
- 负责 QS Top 200 大学参考数据的标准化、入库和匹配。
- 支持按邮箱域名或机构名去匹配 QS 学校。
- 这是机构增强能力，不参与作者-邮箱主对齐。

### `core/fortune500.py`
- 负责 Fortune 500 公司数据的标准化、入库和匹配。
- 支持按邮箱域名或机构名去匹配 Fortune 公司。
- 同样不参与作者-邮箱主对齐。

### `core/similarity_threshold.py`
- 是一个离线评估脚本，用于观察 `_calculate_match_score()` 的阈值表现。
- 不是运行时主流程的一部分。

## 3. 主流程总览：一条记录是怎么生成出来的

当前项目把“作者记录生成”拆成两段：

1. `concurrency.run_etl()` 负责并发框架、建库和写库。
2. `etl_worker.process_file()` 负责逐行读取并调用 `parsing.parse_and_align_authors()` 生成作者-邮箱配对结果。

也就是说，真正决定“这一行会生成几个作者记录、每个作者对应哪个邮箱”的核心代码，主要在：

- `core/etl_worker.py`
- `core/parsing.py`

## 4. 从输入文件到数据库的详细步骤

### 第 1 步：扫描文件并初始化数据库
入口在 `core.concurrency.run_etl()`。

它会先做这些事：

- 递归扫描输入目录下的 `.xls`、`.xlsx`、`.csv` 文件；
- 计算并发 Worker 数量；
- 初始化 SQLite 表结构；
- 自动确保 `qs_universities` 和 `fortune_companies` 两张参考表是最新的；
- 启动多个 Worker 进程处理文件；
- 启动一个单独的 Writer 进程，专门写 SQLite。

这里有一个非常重要的约束：**只有 Writer 进程写数据库**。  
也就是说，作者解析和邮箱匹配发生在 Worker 进程里，但最终入库由单独进程统一完成，避免 SQLite 写锁冲突。

### 第 2 步：读取单个文件
单文件处理入口在 `core.etl_worker.process_file()`。

读取逻辑如下：

- `.csv`：依次尝试 `utf-8`、`gbk`、`gb2312`、`latin-1`；
- `.xls`：使用 `xlrd`；
- `.xlsx`：交给 `pandas.read_excel()` 自动选择引擎。

如果文件读取失败，会直接往 `errors` 表对应的批次里写入 `FILE_READ_ERROR`，该文件不再继续。

### 第 3 步：检查关键列并逐行处理
对每一行，`process_file()` 至少要求以下列存在：

- `Reprint Addresses`
- `Email Addresses`

其余列是可选的：

- `Addresses`
- `Author Full Names`
- `WoS Categories`
- `Research Areas`

如果 `Reprint Addresses` 或 `Email Addresses` 为空，该行不会生成记录，而是记入 `errors` 表，原因是：

- `SKIP: empty reprint or email`

### 第 4 步：先做邮箱预清洗
在正式对齐之前，`process_file()` 先把 `Email Addresses` 按分号切开，然后做一个很明确的过滤：

- 如果邮箱前缀（`@` 前面的部分）是纯数字，则该邮箱直接跳过；
- 该跳过会记录到 `errors` 表，原因是：
  - `SKIP: email_prefix_numeric`

例如：

- `123456@xxx.edu` 会被认为不参与作者匹配；
- 但 `abc123@xxx.edu` 会保留。

如果一行的邮箱全部被过滤掉，这一整行不会再进入后续匹配。

## 5. 作者是如何“生成”的

这里的“生成作者”并不是凭空造出一个作者对象，而是从输入字段中抽出候选作者，然后和邮箱对齐，最后形成 `AuthorContact` 记录。

### 5.1 主要作者来源：`Reprint Addresses`
主入口函数是 `core.parsing.parse_and_align_authors(reprint_str, email_str, addresses_str=None)`。

它首先把 `Reprint Addresses` 拆成多个片段。这里不是简单 `split(";")`，而是：

- 只在圆括号 `()` 外按分号切分；
- 避免机构详情内部的分号把一个作者片段误切开。

也就是说，`Reprint Addresses` 是作者候选的第一来源。

然后对每个片段调用：

- `extract_short_name_from_reprint_segment()`

该函数的规则是：

- 优先取第一个 `(` 之前的内容；
- 去掉尾部多余标点；
- 如果本来就是 `Surname, Initials` 结构，直接整理成标准格式；
- 如果不是逗号格式，就退化为：
  - 第一个词当姓；
  - 后续词取首字母拼成缩写。

示例：

- `Zhou, QY (Shanghai Univ of ...)` -> `Zhou, QY`
- `Xu Cheng` -> `Xu, C`

因此，**主流程中最基础的作者标识并不是全名，而是简称 `short_name`**。

### 5.2 国家来自哪里
国家主要也是从 `Reprint Addresses` 片段里提取，使用：

- `extract_country_from_reprint_segment()`

规则大致是：

- 先移除括号内内容；
- 把 `;` 和 `.` 统一当成分隔符处理；
- 再按逗号拆分并从后往前找最后一个像国家/地区的字段；
- 清除邮编和无关符号。

一个很重要的补偿逻辑是：

- 如果某个 `Reprint` 片段只有作者名，没有足够地址细节；
- 代码会尝试“借用后面第一个有地址细节的片段”来提取国家。

也就是说，作者简称和国家并不一定都来自同一个原始片段：

- 作者简称来自当前片段；
- 国家可能来自“向后借用”的地址片段。

### 5.3 更完整的作者来源：`Addresses`
当 `Addresses` 列存在时，代码会额外尝试从中拿到更完整的信息，使用：

- `extract_authors_from_addresses()`

这个函数会：

- 只在方括号 `[]` 外按分号切分；
- 每个段里提取方括号中的作者名；
- 提取该段对应的国家和机构。

例如：

- `[Duan, Jinyun] East China Normal Univ, Shanghai, Peoples R China`

会被解析成：

- 作者全名：`Duan, Jinyun`
- 国家：`Peoples R China`
- 机构：`East China Normal Univ`

如果一个方括号里有多个作者，例如：

- `[Name1; Name2] Institution, Country`

代码会把它们拆成多个作者条目，但共享同一段里的国家和机构。

因此，`Addresses` 列在当前代码里承担两个作用：

1. 为多邮箱场景提供更可靠的“作者全名 + 机构 + 国家”；
2. 为单邮箱场景补充 `full_name` 和 `institution`。

## 6. 作者名称和邮箱是如何匹配的

### 6.1 相似度的核心计算方法
核心函数是：

- `core.parsing._calculate_match_score(author_name, email)`

它不是只拿“完整作者名”和邮箱前缀硬比，而是会构造多种常见邮箱命名模式，再取最大得分。

处理步骤：

1. 先取邮箱前缀，例如 `chengxu@uni.edu` -> `chengxu`；
2. 对作者名做字母/数字归一化；
3. 计算基础相似度；
4. 再额外生成多种姓名变体：
   - 姓 + 名：`xucheng`
   - 名 + 姓：`chengxu`
   - 名字首字母 + 姓：`cxu` 或 `ltyang`
   - 姓 + 名字首字母：`xuc`
5. 对每一种变体都和邮箱前缀做 `difflib.SequenceMatcher` 相似度；
6. 返回最大值。

所以当前代码的匹配思想是：

- **不是精确规则匹配**；
- **而是“多变体生成 + 字符串相似度打分 + 取最大值”**。

### 6.2 单邮箱场景
如果一行只有 1 个邮箱，`parse_and_align_authors()` 会分两种情况。

#### 情况 A：1 个作者 + 1 个邮箱
- 直接配成一条 `AuthorContact`；
- `short_name` 来自 `Reprint Addresses`；
- `country` 来自 `Reprint Addresses`；
- `similarity` 仍然会计算；
- 如果有 `Addresses`，还会从中找与该邮箱最像的作者，补充：
  - `full_name`
  - `institution`

#### 情况 B：多个作者 + 1 个邮箱
- 代码会遍历所有作者候选；
- 计算每个作者与这个邮箱的相似度；
- 选择得分最高的那个作者；
- 最终只返回 1 条记录。

也就是说，**单邮箱场景下一行最多只会生成 1 条作者记录**。

### 6.3 多邮箱场景：优先使用 `Addresses`
如果一行里有多个邮箱，代码优先尝试使用 `Addresses` 中解析出的作者全名列表。

条件是：

- `addresses_str` 非空；
- 且 `extract_authors_from_addresses()` 能成功提取出作者列表。

此时匹配流程是：

1. 先对 `Addresses` 提取出的作者做一次局部去重；
2. 列出“每个作者 vs 每个邮箱”的所有组合；
3. 对每个组合计算相似度；
4. 按相似度从高到低排序；
5. 做贪心一对一匹配：
   - 一个作者最多匹配一个邮箱；
   - 一个邮箱最多匹配一个作者。

这本质上是一个简化版二部图匹配，但当前实现不是匈牙利算法，而是：

- **先全量打分**
- **再按分数降序贪心选取**

最后结果会按作者原始顺序输出。

在这个分支中：

- `short_name` 实际上直接使用 `Addresses` 中的名字；
- `full_name` 也直接等于这个名字；
- `country` 和 `institution` 也都来自 `Addresses`。

所以这里虽然字段名叫 `short_name`，但如果走了 `Addresses` 分支，它里边放的很可能已经是较完整姓名，而不是严格意义上的“简称”。

### 6.4 多邮箱场景：如果 `Addresses` 不可用，就回退到 `Reprint`
如果 `Addresses` 为空，或者没能提取出有效作者列表，代码就回到 `Reprint Addresses` 的作者简称列表。

此时流程与上面类似：

1. 生成所有 `作者简称 x 邮箱` 组合；
2. 计算相似度；
3. 按分数降序排序；
4. 做贪心一对一匹配；
5. 按原作者顺序返回。

区别是：

- 这里作者侧只有 `short_name`；
- `full_name` 暂时为空；
- 机构字段也没有。

### 6.5 全名回填：`Author Full Names`
如果 `parse_and_align_authors()` 返回的 `AuthorContact` 没有 `full_name`，`etl_worker.process_file()` 会继续调用：

- `map_short_to_full_names(short_names, author_full_names_str)`

这个函数的作用是：把简称映射到 `Author Full Names` 列中的全名。

规则分两层：

1. 先按“姓 + 名字首字母”精确匹配；
2. 如果没匹配上，但同姓候选只有 1 个，则退化成按姓匹配。

如果仍然无法唯一确定，就返回 `None`。

因此，全名的优先级是：

1. 如果 `Addresses` 已经给出了作者名，则优先直接用它；
2. 否则再尝试从 `Author Full Names` 做简称到全名映射；
3. 如果还不行，就保留空字符串。

## 7. 当前代码中的去重机制

去重在当前项目里不是单一动作，而是分成三层，且只有其中一层发生在主匹配流程内部。

### 7.1 解析期局部去重：只发生在 `Addresses` 多邮箱分支
函数：

- `_dedupe_addresses_authors()`

它只对 `extract_authors_from_addresses()` 得到的作者列表去重，规则是：

- key 为“作者名归一化空白后再转大写”；
- 保留首次出现顺序；
- 如果重复出现：
  - 国家优先保留非空值；
  - 若都非空，则保留更长、更“信息丰富”的值；
  - 机构同理。

这一步的目的不是全局数据库去重，而是避免：

- 同一个作者在 `Addresses` 里重复出现；
- 导致一个作者被分配到多个邮箱。

也就是说，这一步是**匹配前的局部去重**。

### 7.2 主 ETL 入库阶段：没有做全局去重
当前 `process_file()` 在生成 `records_batch` 后，会直接交给 Writer 进程写库。

默认 ETL 过程中：

- 不会检查数据库里是否已经存在相同 `(short_name, email)`；
- 不会检查跨文件重复；
- 不会检查同一邮箱是否已被上一行使用；
- 也不会在 Writer 进程做去重。

所以要特别明确：

- **主清洗入库不是“去重入库”，而是“原样累积入库”。**

### 7.3 数据库普通去重：按 `(short_name, email)` 保留最早记录
`core.exporter.py` 中提供了两个普通去重函数：

- `remove_duplicates(db_path)`：直接修改原库；
- `remove_duplicates_to_new_db(db_path)`：不改原库，生成新库。

实际 GUI 当前调用的是：

- `remove_duplicates_to_new_db()`

规则是：

- 以 `(short_name, email)` 为去重键；
- 每组保留 `id` 最小的一条；
- 删除或丢弃其余重复项。

这是一种**结构化去重**，适合处理完全重复的作者-邮箱组合。

### 7.4 精细化去重：按邮箱保留相似度最高记录
`core.exporter.py` 还提供：

- `deduplicate_by_similarity(db_path)`

实际 GUI 也暴露了这个能力。

规则是：

- 以 `email` 为分组键；
- 每组保留 `similarity` 最高的一条；
- 如果相似度相同，则保留 `id` 最小的一条；
- 输出到一个新的数据库文件。

这种去重并不要求 `(short_name, email)` 完全一致，而是认为：

- 一个邮箱理论上应该只归属一个最可信的作者；
- 如果同邮箱对应了多个候选作者，就保留相似度最高者。

这是一种**基于匹配质量的后处理去重**。

## 8. 当前“作者生成 + 匹配 + 去重”流程的真实顺序

把主链路按实际代码顺序展开，大致是：

1. 扫描 Excel/CSV 文件；
2. 每个文件逐行读取；
3. 取出 `Reprint Addresses` 和 `Email Addresses`；
4. 过滤邮箱前缀为纯数字的邮箱；
5. 解析 `Reprint Addresses`，生成作者简称候选；
6. 必要时从后续片段借地址信息来补国家；
7. 如果存在 `Addresses`，解析出作者全名、国家、机构；
8. 如果是多邮箱且 `Addresses` 可用：
   - 先对 `Addresses` 作者做局部去重；
   - 再按相似度做作者-邮箱贪心一对一匹配；
9. 如果 `Addresses` 不可用：
   - 用 `Reprint` 的作者简称与邮箱做贪心一对一匹配；
10. 如果只有一个邮箱：
   - 选相似度最高的作者；
11. 若匹配结果里还没有 `full_name`：
   - 再用 `Author Full Names` 做简称到全名的映射；
12. 计算补充字段：
   - `ethnic_chinese`
   - `institution_norm`
   - `email_domain`
   - `similarity`
13. 直接写入 SQLite `records` 表；
14. 如需真正数据库去重，必须由后续显式调用去重函数完成。

## 9. 几个容易误解、但代码里很关键的点

### 9.1 “作者生成”不是只靠一个字段
当前代码实际上组合使用三个来源：

- `Reprint Addresses`：主作者候选来源，负责简称和基础国家；
- `Addresses`：更完整的作者名、机构和更稳定的国家来源；
- `Author Full Names`：在缺少全名时做补全。

### 9.2 `short_name` 字段不总是“简称”
如果匹配走的是 `Addresses` 分支，那么 `AuthorContact.short_name` 实际会直接填 `Addresses` 中的姓名。  
所以数据库里的 `short_name` 在当前实现中有时是简称，有时更接近全名。

### 9.3 当前匹配算法是贪心，不是全局最优匹配
多邮箱场景下的做法是：

- 全量打分；
- 分数降序；
- 逐个占坑。

这通常够用，但理论上不一定等价于全局最优分配。

### 9.4 主 ETL 并不会自动全局去重
当前入库策略是先保留结果，再通过后处理去重。  
所以如果用户没有主动执行去重，数据库可能保留重复的作者-邮箱记录。

### 9.5 `normalize_short_name_key()` 当前没有进入主流程
`core.parsing.py` 中定义了 `normalize_short_name_key()`，其设计目的看起来是为简称构造统一去重 key，  
但在当前仓库代码里，这个函数没有被主流程调用。

## 10. 各脚本与“作者生成 / 匹配 / 去重”的关系总结

如果只看这三个主题，可以简化成下面这张职责图：

- 作者生成：
  - `etl_worker.py`
  - `parsing.py`
- 作者-邮箱匹配：
  - `parsing.py`
- 主流程入库：
  - `concurrency.py`
  - `etl_worker.py`
- 主流程中的局部去重：
  - `parsing.py::_dedupe_addresses_authors`
- 数据库后处理去重：
  - `exporter.py::remove_duplicates_to_new_db`
  - `exporter.py::deduplicate_by_similarity`
- 邮箱有效性后处理：
  - `email_validation.py`
- 机构增强匹配：
  - `qs200.py`
  - `fortune500.py`

## 11. 结论

当前项目的核心匹配思想可以概括为：

- 用 `Reprint Addresses` 生成作者简称候选；
- 优先用 `Addresses` 提供更完整的作者名、国家、机构；
- 用“姓名多变体 vs 邮箱前缀”的字符串相似度做一对一匹配；
- 用 `Author Full Names` 对缺失全名的结果做补全；
- 主 ETL 过程中只做局部解析期去重，不做数据库级全局去重；
- 真正的数据库去重由 `exporter.py` 中的专门函数在后处理阶段完成。

如果只回答三个核心问题，那么当前代码的答案是：

- **如何生成作者**：主要从 `Reprint Addresses` 生成作者简称，从 `Addresses`/`Author Full Names` 补全完整姓名与机构。
- **如何匹配作者和邮箱**：通过 `_calculate_match_score()` 计算姓名变体与邮箱前缀相似度，再做贪心一对一匹配。
- **如何去重**：主流程只在 `Addresses` 作者列表上做局部去重；数据库级去重要靠后处理函数，当前主要有 `(short_name, email)` 去重和“按邮箱保留最高相似度”两种。
