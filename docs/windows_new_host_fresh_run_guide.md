# Windows 新主机迁移与从零重跑指南

## 1. 适用场景

本文适用于以下场景：

- 新主机是 Windows；
- 不需要保留旧主机上的 `manifest.db`、`data_pipeline/`、OpenSearch 旧索引或旧容器数据；
- 只需要把当前项目迁到新机器，并从新的源数据目录重新扫描、重新跑完整流程；
- 需要时再把结果重新写入新主机上的 OpenSearch。

本文默认仓库路径为 `D:\CombineDatabase`。如果你的实际路径不同，只需要修改下面命令中的变量。

---

## 2. 当前项目整体流程

当前项目的执行链路是：

1. `scan_and_queue.py scan`
   扫描源目录中的 `xlsx/xlsm/xls/csv` 文件，写入 `manifest.db`，并为后续阶段创建待执行任务。
2. `run_pipeline.py`
   按顺序执行：
   - `scan`
   - `bronze`
   - `silver`
   - `match`
   - `index`
3. `index` 阶段会调用：
   - `create_indices.ps1`
   - `bulk_import.ps1`
   将 Silver/Match 产物导入 OpenSearch。

如果你暂时不需要 OpenSearch，可以先只跑到 `match`，后续再单独补跑 `index`。

---

## 3. 新主机需要准备的环境

### 3.1 必备软件

- Windows 10/11
- Python 3.12+
- PowerShell
- Git

### 3.2 如果要跑 OpenSearch，还需要

- Docker Desktop
- Docker Compose

建议给 Docker Desktop 至少分配：

- 4 GB 内存以上
- 足够的磁盘空间存放索引数据

说明：

- 当前 `docker-compose-opensearch.yml` 使用 `opensearchproject/opensearch:3.5.0`
- 默认暴露端口：
  - `9200`：OpenSearch
  - `5601`：OpenSearch Dashboards

---

## 4. 复制项目到新主机

把整个仓库目录复制到新主机，例如：

```powershell
D:\CombineDatabase
```

建议同时把以下内容一并带过去：

- 所有 `.py` / `.ps1` / `.json` / `.yaml` / `.yml` 源文件
- `docs/`
- 测试夹具：
  - `_scan_test/`
  - `_pipeline_test/`

如果你已经在新主机准备好了新的原始数据目录，那么旧主机上的以下内容都不需要保留：

- `manifest.db`
- `data_pipeline/`
- `output_schema_v1/`
- `output_schema_v1_stream/`
- 各类 `tmp_*.db`
- 各类 `tmp_*` 输出目录

---

## 5. 在新主机创建 Python 环境

下面示例使用 PowerShell。

### 5.1 进入项目目录

```powershell
$ProjectRoot = "D:\CombineDatabase"
Set-Location $ProjectRoot
```

### 5.2 创建虚拟环境

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 5.3 安装依赖

先安装仓库已有依赖：

```powershell
python -m pip install --upgrade pip
python -m pip install -r .\requirements.txt
```

再补装 Excel 读取依赖：

```powershell
python -m pip install openpyxl xlrd
```

说明：

- `requirements.txt` 里目前包含：
  - `pandas`
  - `pyarrow`
  - `ijson`
- 但项目实际要读取：
  - `.xlsx/.xlsm`
  - `.xls`
- 因此新主机建议额外安装：
  - `openpyxl`
  - `xlrd`

### 5.4 验证环境

```powershell
python --version
python -m pip show pandas pyarrow ijson openpyxl xlrd
```

---

## 6. 配置本次运行的目录变量

请把下面变量替换成你新主机上的真实路径。

```powershell
$ProjectRoot = "D:\CombineDatabase"
$DbPath = "$ProjectRoot\manifest.db"
$OutputRoot = "$ProjectRoot\data_pipeline"
$BaseDir = $ProjectRoot

$ScholarsRoot = "D:\Data\scholars"
$FrontiersRoot = "D:\Data\frontiers"
```

检查源目录是否存在：

```powershell
Test-Path $ScholarsRoot
Test-Path $FrontiersRoot
```

都应返回 `True`。

---

## 7. 清理旧状态，确保从零开始

如果你复制仓库时把旧机器上的运行产物也带过来了，建议在新机先清理，避免混淆。

```powershell
Set-Location $ProjectRoot

Remove-Item -Force $DbPath -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force $OutputRoot -ErrorAction SilentlyContinue
```

如果你还带来了旧的临时文件，也可以一并清理：

```powershell
Get-ChildItem $ProjectRoot -Force |
  Where-Object {
    $_.Name -like "tmp_*" -or
    $_.Name -eq "output_schema_v1" -or
    $_.Name -eq "output_schema_v1_stream"
  } |
  Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
```

说明：

- 本方案不保留旧主机的状态；
- `manifest.db` 会在新主机重新生成；
- `data_pipeline/` 也会在新主机重新生成。

---

## 8. 只跑本地 ETL，不接 OpenSearch 的流程

如果你想先确认本地 Python ETL 全部正常，可以先只跑到 `match`。

### 8.1 扫描并创建任务

```powershell
Set-Location $ProjectRoot

python .\scan_and_queue.py scan `
  --db-path $DbPath `
  --scholars-root $ScholarsRoot `
  --frontiers-root $FrontiersRoot `
  --enqueue-stages scan,bronze,silver,match
```

### 8.2 执行主流程

```powershell
python .\run_pipeline.py `
  --db-path $DbPath `
  --base-dir $BaseDir `
  --output-root $OutputRoot `
  --stages scan,bronze,silver,match `
  --storage-format parquet
```

### 8.3 检查输出

```powershell
Get-Content "$OutputRoot\pipeline_run_summary.json"
Get-ChildItem "$OutputRoot\silver" -Directory | Sort-Object Name
Get-ChildItem "$OutputRoot\match" -Directory | Sort-Object Name
```

重点检查：

- `data_pipeline\pipeline_run_summary.json`
- `data_pipeline\silver\<时间戳>\transform_summary.json`
- `data_pipeline\silver\<时间戳>\quality_report.json`
- `data_pipeline\match\<时间戳>\`

如果这一步正常，说明 Python 侧迁移已经完成。

---

## 9. 包含 OpenSearch 的完整流程

如果你要在新主机上把结果重新写入 OpenSearch，请按下面步骤继续。

### 9.1 准备 `.env`

当前 Docker Compose 使用 `.env` 中的 `OPENSEARCH_INITIAL_ADMIN_PASSWORD`。

可以直接在项目根目录生成：

```powershell
$OpenSearchAdminUser = "admin"
$OpenSearchAdminPassword = "ChangeThis_To_A_Strong_Password_123!"

"OPENSEARCH_INITIAL_ADMIN_PASSWORD=$OpenSearchAdminPassword" |
  Set-Content -Encoding utf8 "$ProjectRoot\.env"
```

### 9.2 启动 OpenSearch 和 Dashboards

```powershell
Set-Location $ProjectRoot
docker compose -f .\docker-compose-opensearch.yml up -d
```

查看容器状态：

```powershell
docker compose -f .\docker-compose-opensearch.yml ps
```

### 9.3 验证 OpenSearch 是否可访问

项目里的 Docker 配置开启了安全插件，因此建议直接按 `https` 访问本机 `9200`。

```powershell
curl.exe -k -u "${OpenSearchAdminUser}:$OpenSearchAdminPassword" https://localhost:9200
```

如果返回集群信息 JSON，说明服务正常。

### 9.4 从零扫描，并把所有阶段都加入队列

```powershell
Set-Location $ProjectRoot

python .\scan_and_queue.py scan `
  --db-path $DbPath `
  --scholars-root $ScholarsRoot `
  --frontiers-root $FrontiersRoot `
  --enqueue-stages scan,bronze,silver,match,index
```

### 9.5 运行完整主流程

```powershell
python .\run_pipeline.py `
  --db-path $DbPath `
  --base-dir $BaseDir `
  --output-root $OutputRoot `
  --stages scan,bronze,silver,match,index `
  --storage-format parquet `
  --opensearch-endpoint https://localhost:9200 `
  --opensearch-username $OpenSearchAdminUser `
  --opensearch-password $OpenSearchAdminPassword `
  --opensearch-insecure
```

说明：

- `--opensearch-endpoint` 建议使用 `https://localhost:9200`
- `--opensearch-insecure` 用于跳过本地自签名证书校验
- 当前 `index` 阶段会自动调用：
  - `create_indices.ps1`
  - `bulk_import.ps1`

你不需要单独再手工执行这两个脚本，除非是在排查问题。

---

## 10. 完整流程的一次性命令模板

如果你已经确认环境、源目录、Docker 都已准备好，可以按下面顺序直接执行。

```powershell
$ProjectRoot = "D:\CombineDatabase"
$DbPath = "$ProjectRoot\manifest.db"
$OutputRoot = "$ProjectRoot\data_pipeline"
$BaseDir = $ProjectRoot

$ScholarsRoot = "D:\Data\scholars"
$FrontiersRoot = "D:\Data\frontiers"

$OpenSearchAdminUser = "admin"
$OpenSearchAdminPassword = "ChangeThis_To_A_Strong_Password_123!"

Set-Location $ProjectRoot

python -m venv .venv
.\.venv\Scripts\Activate.ps1

python -m pip install --upgrade pip
python -m pip install -r .\requirements.txt
python -m pip install openpyxl xlrd

"OPENSEARCH_INITIAL_ADMIN_PASSWORD=$OpenSearchAdminPassword" |
  Set-Content -Encoding utf8 "$ProjectRoot\.env"

Remove-Item -Force $DbPath -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force $OutputRoot -ErrorAction SilentlyContinue

docker compose -f .\docker-compose-opensearch.yml up -d

python .\scan_and_queue.py scan `
  --db-path $DbPath `
  --scholars-root $ScholarsRoot `
  --frontiers-root $FrontiersRoot `
  --enqueue-stages scan,bronze,silver,match,index

python .\run_pipeline.py `
  --db-path $DbPath `
  --base-dir $BaseDir `
  --output-root $OutputRoot `
  --stages scan,bronze,silver,match,index `
  --storage-format parquet `
  --opensearch-endpoint https://localhost:9200 `
  --opensearch-username $OpenSearchAdminUser `
  --opensearch-password $OpenSearchAdminPassword `
  --opensearch-insecure
```

---

## 11. 运行完成后的检查项

### 11.1 检查主流程汇总

```powershell
Get-Content "$OutputRoot\pipeline_run_summary.json"
```

### 11.2 检查 Bronze

```powershell
Get-ChildItem "$OutputRoot\bronze"
```

预期至少会看到：

- `scholars.rows.parquet` 或 `scholars.rows.jsonl`
- `frontiers.rows.parquet` 或 `frontiers.rows.jsonl`

### 11.3 检查最新 Silver 输出

```powershell
$LatestSilver = Get-ChildItem "$OutputRoot\silver" -Directory | Sort-Object Name | Select-Object -Last 1
$LatestSilver.FullName
Get-Content "$($LatestSilver.FullName)\transform_summary.json"
Get-Content "$($LatestSilver.FullName)\quality_report.json"
```

### 11.4 检查最新 Match 输出

```powershell
$LatestMatch = Get-ChildItem "$OutputRoot\match" -Directory | Sort-Object Name | Select-Object -Last 1
$LatestMatch.FullName
Get-ChildItem $LatestMatch.FullName
```

### 11.5 检查 OpenSearch 索引

```powershell
curl.exe -k -u "${OpenSearchAdminUser}:$OpenSearchAdminPassword" https://localhost:9200/_cat/indices?v
```

如果索引创建和导入都成功，通常会看到类似：

- `publication_v1`
- `author_occurrence_v1`
- `author_identifier_claim_v1`
- `author_affiliation_claim_v1`
- `author_email_claim_v1`
- `person_v1`
- `person_publication_v1`

---

## 12. 常见问题

### 12.1 `ModuleNotFoundError: openpyxl` 或 `ImportError: Missing optional dependency 'openpyxl'`

说明新主机没有安装 Excel 引擎。

处理：

```powershell
python -m pip install openpyxl
```

### 12.2 读取 `.xls` 报错

说明缺少 `xlrd`。

处理：

```powershell
python -m pip install xlrd
```

### 12.3 `Parquet support requires pyarrow`

说明 `pyarrow` 没装好。

处理：

```powershell
python -m pip install pyarrow
```

### 12.4 `Streaming parser dependency missing: ijson`

说明 `ijson` 没装好。

处理：

```powershell
python -m pip install ijson
```

### 12.5 OpenSearch 连不上

优先检查：

1. Docker Desktop 是否已启动
2. `docker compose ... ps` 是否显示容器在运行
3. 是否使用了正确的管理员密码
4. 是否使用 `https://localhost:9200`
5. 是否在命令里带了 `--opensearch-insecure`

### 12.6 想重跑失败任务

如果只是某次运行失败，不想重新全量扫描，可以用：

```powershell
python .\scan_and_queue.py rerun `
  --db-path $DbPath `
  --since 2026-04-07T00:00:00+08:00
```

然后再执行：

```powershell
python .\run_pipeline.py `
  --db-path $DbPath `
  --base-dir $BaseDir `
  --output-root $OutputRoot `
  --stages bronze,silver,match,index `
  --storage-format parquet `
  --opensearch-endpoint https://localhost:9200 `
  --opensearch-username $OpenSearchAdminUser `
  --opensearch-password $OpenSearchAdminPassword `
  --opensearch-insecure
```

---

## 13. 推荐执行顺序

建议按下面顺序执行，最稳妥：

1. 在新主机安装 Python、Docker Desktop
2. 复制仓库到新目录
3. 创建虚拟环境并安装依赖
4. 清空旧的 `manifest.db` 和 `data_pipeline/`
5. 先只跑到 `match`，确认 Python 流程正常
6. 再启动 OpenSearch
7. 最后跑包含 `index` 的完整流程

这样做的好处是：

- 可以先把“Python 环境问题”和“OpenSearch 环境问题”拆开；
- 出现故障时更容易定位是 ETL 端还是索引端；
- 不会把多个问题混在一起排查。
