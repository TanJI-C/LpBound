# LpBound 接入与使用指南 (Planner 实现参考)

本指南总结了在真实场景中（如自定义 `Planner` 类）接入 LpBound 进行基数估计与 PostgreSQL hint 注入的完整流程，以及跑新数据集时的配置清单和避坑指南。

## 1. 核心运行管线

LpBound 的运行本质上分为两大阶段：**离线统计构建** 与 **在线估计及注入**。

### 1.1 离线统计构建 (Offline Statistics Building)
系统需要读取 CSV 数据分布，预计算 $L_p$ 范数、MCV 等统计信息，并缓存到 DuckDB 中。

- **入口 API**: `build_lpbound_statistics(config)`
  （详见 `src/lpbound/acyclic/lpbound.py`）
- **核心流程**:
  1. 加载 `LpBoundConfig` 与 Benchmark Schema。
  2. 初始化 DuckDB（执行 `create_queries_{db}.sql` 并 `COPY` 导入 CSV 数据）。
  3. 生成统计 SQL（如 `*_lpnorm_queries.sql`），提取 MCV、直方图及 Join 键的 $L_p$ 范数。
  4. 执行这些 SQL，结果写入 DuckDB 的 `norms` 等内部视图中。

### 1.2 在线估计及注入 (Online Estimation & Injection)
逐条处理真实查询，预估基数，并转化为 PostgreSQL 认识的 hints 格式。

- **单查询估计 API**: `estimate(input_query_sql, config, dump_lp_program_file=None, verbose=False)`
- **核心流程**:
  1. **SQL 解析与 Join 图构建**: 通过 `sqlparse` 提取表、连接条件及过滤谓词，构建 JoinGraph。
  2. **拉取统计**: 根据 Join 图去 DuckDB 中检索 MCV 与 $L_p$ 范数。
  3. **LP 求解**: 使用 OR-Tools 构建线性规划问题，寻找满足统计约束的“最大可能结果集大小”（Pessimistic Bound）。
  4. **Hint 注入**:
     - 对查询涉及的子查询集合调用 `estimate`。
     - 组装 `pg_hint_plan` 的 `Rows(t1 t2 #estimate)` 格式注释块。
     - 执行：`executeQueryWithHints(sql, hints)` （详见 `benchmarks/approaches/postgres/postgres.py`）。

---

## 2. 跑新数据集配置清单

要在一套新的数据集（如 `my_dataset`）上跑通 LpBound，需要准备以下物料及配置修改：

### 2.1 数据与基础脚本
1. **CSV 数据文件**: 必须放置在 `data/datasets/my_dataset/*.csv` 下。确保分隔符、表头处理在配置中能对应上。
2. **DuckDB 建表脚本**: 创建 `data/sql_scripts/duckdb/create_queries_my_dataset.sql`。定义所有表的结构，LpBound 会根据它来初始化 DuckDB。
3. **DuckDB 索引脚本 (可选)**: 如果需要加速 DuckDB 查询，可创建 `data/sql_scripts/duckdb/create_indexes_my_dataset.sql`。
4. **Workload 查询文件**: 准备包含测试 SQL 的文件，通常放在 `benchmarks/workloads/my_dataset/` 或自定义路径下。

### 2.2 Schema 文件
在 `benchmarks/schemas/my_dataset_schema.json` 中定义元数据，核心包含：
- `"name"`: 数据集名称。
- `"relations"`: 表名及其主键、外键约束、带谓词的列信息（这是生成统计的基础）。

### 2.3 关键代码映射修改
必须修改 `src/lpbound/config/paths.py` 中的映射字典，告诉框架如何找文件：
- `WORKLOAD_TO_DB_MAP`: 添加 `"my_dataset": "my_dataset"`。这决定了生成的 DuckDB 库文件名为 `my_dataset_duckdb.db`。
- `CSV_DATA_DIR_MAP`: 添加 `"my_dataset": "data/datasets/my_dataset"`。

### 2.4 PostgreSQL 评测配置
如果要执行真实评测并注入 Hint，还需修改 `src/lpbound/config/postgres_config.py` 或相关测试入口：
- 确保 `pg_hint_plan` 插件已在 Postgres 中安装并可通过 `LOAD 'pg_hint_plan'` 加载。
- 配置 Postgres 的连接参数（dbname, user, port 等）。

---

## 3. 避坑事项 (Troubleshooting)

在对接与调试过程中，以下几点极易踩坑：

### 3.1 DuckDB 与 PostgreSQL 数据必须绝对对齐
- **现象**: 执行 `estimate` 时出现 `TypeError: int() argument must be a string, a bytes-like object or a number, not 'NoneType'`。
- **原因**: 框架在 DuckDB（用作统计库）中查不到特定谓词匹配的行，导致聚合函数（如 `MIN`）返回 `NULL`（Python 中为 `None`），而下游代码没做容错。这通常是因为 DuckDB 库复用了旧的/不完整的 db 文件（例如伪装成 `joblight` 但缺失了表），而 PostgreSQL 中有真实数据。
- **解决**: 
  - 确保 `paths.py` 的映射正确。
  - 删除旧的 `.db` 文件，让系统重新执行建表并从完整的 CSV `COPY` 导入数据。

### 3.2 不支持的复杂 SQL 谓词 (LIKE/OR/子查询等)
- **现象**: 查询解析失败，或提取的谓词为空，导致估计无限大（`inf`）或抛错。
- **原因**: LpBound 当前的 Acyclic SQL 解析器（基于 `sqlparse`）对 `LIKE`、`OR`、复杂的括号嵌套支持有限。它主要面向等值 Join 和简单的等值/范围谓词。
- **解决**: 
  - 在 `Planner` 中加入预处理：去除/忽略 `LIKE` 等谓词（虽然会使估计变宽）。
  - 将 `BETWEEN AND` 运算符转换为范围谓词（如 `>=`、`<=`）。
  - 实现回退策略（Fallback）：在解析异常或遇到不支持的语法时，捕获异常并回退到 PostgreSQL 原生的 `EXPLAIN` 估计。

### 3.3 补 0 导致的数值异常与无界 (Infinity)
- **现象**: 修改源码把统计 `None` 强行填 `0` 后，虽然不崩了，但大量查询返回 `inf` 估计。
- **原因**: LP 约束大量依赖 `log2(norm)`。如果填 `0`，`log2(0)` 会变成 `-inf`，导致核心约束退化失效，求解器找不到有效上界。
- **解决**: 
  - 兜底方案应优先考虑回退到 `NOPRED`（无谓词）的统计。
  - 如果真要硬性兜底，使用 `1.0`（因为 `log2(1)=0`）比用 `0` 稳妥，至少不会引发除零或 `-inf`。

### 3.4 Hint 注入的粒度
- **现象**: 注入了 Hint 但执行计划没变。
- **原因**: `pg_hint_plan` 的 `Rows(...)` 必须精准对应执行计划中的中间节点。只给最外层一个数字不够，你需要通过递归枚举连接子图，为关键的子查询组合分别调 `estimate` 并生成 `JoinHint`。
- **参考**: 查看 `benchmarks/experiments/evaluation_time.py` 中的 `compute_subquery_cardinalities` 方法。