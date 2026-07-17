# OpenSearch 中文字段检索服务

## 目标
- 基于现有 `schema_v1.yaml` 与 `opensearch_mapping_v1.json` 自动生成字段目录
- 为 7 个实体索引提供结构化筛选 API
- 提供托管式前端页面，支持字段选择、结果表、DSL/SQL 预览和异步导出

## 安装依赖

```powershell
python -m pip install -r .\requirements.txt
```

## 启动服务

默认读取以下环境变量：
- `OPENSEARCH_ENDPOINT`
- `OPENSEARCH_USERNAME`
- `OPENSEARCH_PASSWORD`
- `OPENSEARCH_INSECURE`
- `SEARCH_API_HOST`
- `SEARCH_API_PORT`

如果项目根目录存在 `.env`，服务会自动加载其中尚未注入到进程环境的变量。

本地默认值：
- `OPENSEARCH_ENDPOINT=https://localhost:9200`
- `OPENSEARCH_USERNAME=admin`
- `OPENSEARCH_PASSWORD` 优先读取显式环境变量，否则回退到 `.env` 中的 `OPENSEARCH_INITIAL_ADMIN_PASSWORD`
- `OPENSEARCH_INSECURE=true`

本地启动命令：

```powershell
python .\run_search_api.py
```

或直接使用 Uvicorn：

```powershell
uvicorn search_app.main:app --host 0.0.0.0 --port 8010
```

启动后访问：
- 本机：`http://127.0.0.1:8010/`
- 局域网其他设备：`http://<当前电脑IP>:8010/`

### Windows 防火墙放行

如果同网段其他设备无法访问，请为当前服务放行 TCP `8010` 入站规则：

```powershell
New-NetFirewallRule `
  -DisplayName "OpenSearch Search API 8010" `
  -Direction Inbound `
  -Action Allow `
  -Protocol TCP `
  -LocalPort 8010
```

查看本机局域网 IP：

```powershell
ipconfig
```

常见说明：
- 默认启动地址为 `0.0.0.0:8010`，表示监听所有网卡
- 前端静态资源和 API 同源提供，不需要额外配置 CORS
- 上次筛选条件和自定义场景仅保存在浏览器本地 `localStorage`

## 主要接口

- `GET /api/search/entities`
- `GET /api/search/entities/{entity}/fields`
- `POST /api/search/query`
- `POST /api/search/count`
- `POST /api/search/export`
- `GET /api/search/export/jobs/{job_id}`
- `GET /api/search/export/jobs/{job_id}/download`

## 前端本地缓存

浏览器会自动保存并恢复以下本地状态：
- 当前实体
- 当前实体的筛选条件
- 当前实体的结果列
- 当前实体的排序字段与排序方向
- 每页条数
- 最近一次统计数量

使用的本地存储键：
- `os_search:last_state:v1`
- `os_search:scenes:v1`

说明：
- “恢复默认”只清除当前实体的本地缓存，不影响其他实体
- 自定义场景只保存在当前浏览器，不会同步到服务端或其他局域网设备

## 查询体示例

```json
{
  "entity": "publication",
  "query_tree": {
    "type": "group",
    "logic": "and",
    "children": [
      { "type": "rule", "field": "doi", "operator": "eq", "value": "10.1000/demo" },
      {
        "type": "rule",
        "field": "publication_year",
        "operator": "between",
        "range": { "from": 2020, "to": 2024 }
      }
    ]
  },
  "select_fields": ["publication_id", "title", "publication_year"],
  "sort": [{ "field": "publication_year", "order": "desc" }],
  "page": 1,
  "page_size": 20
}
```

## 测试

```powershell
python -m unittest discover -s .\tests -p "test_*.py"
```
