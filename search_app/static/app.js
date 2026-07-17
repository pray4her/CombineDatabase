const { createApp, reactive, ref, computed, onMounted, watch } = Vue;

const STORAGE_KEYS = {
  lastState: "os_search:last_state:v1",
  scenes: "os_search:scenes:v1",
};

const STORAGE_VERSION = 1;

const OPERATOR_LABELS = {
  eq: "等于",
  neq: "不等于",
  in: "包含任一",
  not_in: "不包含任一",
  exists: "有值",
  not_exists: "无值",
  prefix: "前缀匹配",
  match: "模糊匹配",
  match_phrase: "短语匹配",
  gt: "大于",
  gte: "大于等于",
  lt: "小于",
  lte: "小于等于",
  between: "区间",
};

const OPERATOR_HELP_TEXT = {
  eq: "精确匹配一个值，适合编号、国家、年份等确定字段。",
  neq: "排除某个精确值。",
  in: "多个候选值中命中任意一个即可。",
  not_in: "排除多个候选值。",
  exists: "仅保留该字段有内容的记录。",
  not_exists: "仅保留该字段为空或不存在的记录。",
  prefix: "按前缀匹配，适合编号、邮箱域名等。",
  match: "全文模糊匹配，适合标题、摘要、机构等文本字段。",
  match_phrase: "按连续短语匹配，结果更严格。",
  gt: "字段值必须大于输入值。",
  gte: "字段值必须大于或等于输入值。",
  lt: "字段值必须小于输入值。",
  lte: "字段值必须小于或等于输入值。",
  between: "输入起始和结束范围进行筛选。",
};

const HELP_TEXT = {
  entity:
    "先选择检索实体，再在该实体的字段范围内组合筛选。不同实体会分别记住上次的列、排序和筛选条件。",
  columns: "选择结果表需要展示的字段。这里的设置会保存在当前浏览器，下次打开自动恢复。",
  sortField: "排序字段只显示当前实体中支持排序的字段。",
  sortOrder: "控制结果表的升序或降序显示。",
  logic: "条件组支持“满足全部条件”或“满足任一条件”，可组合多层结构。",
  operator: "操作符会根据字段类型自动限制为可用的中文选项。",
  scenes:
    "场景会把当前实体、筛选条件、展示列、排序和每页条数保存到当前浏览器，方便下次一键恢复。",
  preview: "这里展示后端最终生成的 DSL / SQL 预览，便于排查或校验查询逻辑。",
};

function createRule() {
  return {
    type: "rule",
    field: "",
    operator: "",
    value: "",
    valuesText: "",
    range: { from: "", to: "" },
    nested_path: null,
  };
}

function createGroup() {
  return {
    type: "group",
    logic: "and",
    children: [createRule()],
  };
}

function cloneData(value) {
  return JSON.parse(JSON.stringify(value));
}

function readStorage(key, fallbackValue) {
  try {
    const rawValue = window.localStorage.getItem(key);
    if (!rawValue) {
      return cloneData(fallbackValue);
    }
    return JSON.parse(rawValue);
  } catch (error) {
    console.warn(`读取本地存储失败: ${key}`, error);
    return cloneData(fallbackValue);
  }
}

function writeStorage(key, payload) {
  try {
    window.localStorage.setItem(key, JSON.stringify(payload));
  } catch (error) {
    console.warn(`写入本地存储失败: ${key}`, error);
  }
}

function createDefaultPersistedState() {
  return {
    version: STORAGE_VERSION,
    selectedEntity: "",
    entities: {},
  };
}

function normalizePersistedState(rawState) {
  if (!rawState || typeof rawState !== "object") {
    return createDefaultPersistedState();
  }
  return {
    version: STORAGE_VERSION,
    selectedEntity: typeof rawState.selectedEntity === "string" ? rawState.selectedEntity : "",
    entities: rawState.entities && typeof rawState.entities === "object" ? rawState.entities : {},
  };
}

function normalizeScenes(rawScenes) {
  if (!Array.isArray(rawScenes)) {
    return [];
  }
  return rawScenes
    .filter((scene) => scene && typeof scene === "object" && typeof scene.entity === "string")
    .map((scene) => ({
      id: scene.id || `scene_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
      name: typeof scene.name === "string" ? scene.name : "未命名场景",
      entity: scene.entity,
      queryTree: scene.queryTree || createGroup(),
      selectedColumns: Array.isArray(scene.selectedColumns) ? scene.selectedColumns : [],
      sortField: typeof scene.sortField === "string" ? scene.sortField : "",
      sortOrder: scene.sortOrder === "desc" ? "desc" : "asc",
      pageSize: Number.isInteger(scene.pageSize) && scene.pageSize > 0 ? scene.pageSize : 20,
      createdAt: scene.createdAt || new Date().toISOString(),
      updatedAt: scene.updatedAt || new Date().toISOString(),
    }));
}

function operatorLabel(operator) {
  return OPERATOR_LABELS[operator] || operator;
}

function castValue(value, fieldMeta) {
  if (!fieldMeta) return value;
  if (fieldMeta.field_type === "integer") {
    return value === "" || value === null ? null : Number.parseInt(value, 10);
  }
  if (fieldMeta.field_type === "float") {
    return value === "" || value === null ? null : Number.parseFloat(value);
  }
  if (fieldMeta.field_type === "boolean") {
    if (value === true || value === false) return value;
    return value === "true";
  }
  return value;
}

function valueForRequest(rule, fieldMeta) {
  const base = {
    type: "rule",
    field: rule.field,
    operator: rule.operator,
    nested_path: fieldMeta && fieldMeta.nested_path ? fieldMeta.nested_path : null,
  };

  if (rule.operator === "between") {
    base.range = {
      from: rule.range.from === "" ? null : castValue(rule.range.from, fieldMeta),
      to: rule.range.to === "" ? null : castValue(rule.range.to, fieldMeta),
    };
    return base;
  }

  if (rule.operator === "in" || rule.operator === "not_in") {
    base.values = rule.valuesText
      .split(/[\n,;，；]/)
      .map((item) => item.trim())
      .filter(Boolean)
      .map((item) => castValue(item, fieldMeta));
    return base;
  }

  if (rule.operator === "exists" || rule.operator === "not_exists") {
    return base;
  }

  base.value = castValue(rule.value, fieldMeta);
  return base;
}

function nodeForRequest(node, fieldMap) {
  if (node.type === "group") {
    const children = node.children
      .map((child) => nodeForRequest(child, fieldMap))
      .filter(Boolean);
    return {
      type: "group",
      logic: node.logic,
      children,
    };
  }
  if (!node.field && !node.operator) {
    return null;
  }
  const fieldMeta = fieldMap[node.field] || null;
  return valueForRequest(node, fieldMeta);
}

function formatCell(value, column) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  if (Array.isArray(value)) {
    return value
      .flatMap((item) => (Array.isArray(item) ? item : [item]))
      .map((item) => formatCell(item, column))
      .join("；");
  }
  if (typeof value === "object") {
    return JSON.stringify(value, null, 0);
  }
  if (column && column.table_renderer === "boolean") {
    return value ? "是" : "否";
  }
  return String(value);
}

function createDefaultEntityState(entityMeta) {
  return {
    queryTree: createGroup(),
    selectedColumns: [...(entityMeta.default_visible_fields || [])],
    sortField: entityMeta.default_sort?.[0]?.field || "",
    sortOrder: entityMeta.default_sort?.[0]?.order || "asc",
    pageSize: 20,
    lastCountValue: null,
  };
}

function sanitizeRuleNode(rule, fieldMap) {
  if (!rule || rule.type !== "rule") {
    return null;
  }

  if (!rule.field && !rule.operator) {
    return null;
  }

  const fieldMeta = fieldMap[rule.field];
  if (!fieldMeta) {
    return null;
  }

  const nextRule = createRule();
  nextRule.field = fieldMeta.field_path;
  nextRule.operator = fieldMeta.operators.includes(rule.operator) ? rule.operator : fieldMeta.operators[0];
  nextRule.nested_path = fieldMeta.nested_path || null;

  if (nextRule.operator === "between") {
    const nextRange = rule.range && typeof rule.range === "object" ? rule.range : {};
    nextRule.range = {
      from: nextRange.from ?? "",
      to: nextRange.to ?? "",
    };
    return nextRule;
  }

  if (nextRule.operator === "in" || nextRule.operator === "not_in") {
    if (typeof rule.valuesText === "string") {
      nextRule.valuesText = rule.valuesText;
    } else if (Array.isArray(rule.values)) {
      nextRule.valuesText = rule.values.join("；");
    }
    return nextRule;
  }

  if (nextRule.operator === "exists" || nextRule.operator === "not_exists") {
    return nextRule;
  }

  if (fieldMeta.field_type === "boolean") {
    nextRule.value = typeof rule.value === "boolean" ? rule.value : true;
  } else {
    nextRule.value = rule.value ?? "";
  }
  return nextRule;
}

function sanitizeQueryTree(queryTree, fieldMap) {
  if (!queryTree || queryTree.type !== "group") {
    return createGroup();
  }

  const children = Array.isArray(queryTree.children) ? queryTree.children : [];
  const sanitizedChildren = children
    .map((child) => {
      if (child && child.type === "group") {
        return sanitizeQueryTree(child, fieldMap);
      }
      return sanitizeRuleNode(child, fieldMap);
    })
    .filter(Boolean);

  return {
    type: "group",
    logic: queryTree.logic === "or" ? "or" : "and",
    children: sanitizedChildren.length > 0 ? sanitizedChildren : [createRule()],
  };
}

function applyQueryTree(targetQueryTree, nextQueryTree) {
  targetQueryTree.logic = nextQueryTree.logic || "and";
  targetQueryTree.children.splice(
    0,
    targetQueryTree.children.length,
    ...cloneData(nextQueryTree.children || [createRule()])
  );
}

function createSceneFromState(entity, state, name) {
  const now = new Date().toISOString();
  return {
    id: `scene_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    name,
    entity,
    queryTree: cloneData(state.queryTree),
    selectedColumns: [...state.selectedColumns],
    sortField: state.sortField,
    sortOrder: state.sortOrder,
    pageSize: state.pageSize,
    createdAt: now,
    updatedAt: now,
  };
}

const HelpTip = {
  name: "HelpTip",
  props: {
    content: { type: String, required: true },
  },
  template: `
    <el-tooltip effect="light" placement="top" :content="content">
      <span class="help-icon" tabindex="0" aria-label="帮助">?</span>
    </el-tooltip>
  `,
};

const QueryBuilder = {
  name: "QueryBuilder",
  components: { HelpTip },
  props: {
    node: { type: Object, required: true },
    fields: { type: Array, required: true },
    fieldMap: { type: Object, required: true },
    operatorLabels: { type: Object, required: true },
    operatorHelpText: { type: Object, required: true },
    isRoot: { type: Boolean, default: false },
  },
  emits: ["remove"],
  methods: {
    addRule() {
      this.node.children.push(createRule());
    },
    addGroup() {
      this.node.children.push(createGroup());
    },
    removeChild(index) {
      this.node.children.splice(index, 1);
      if (this.node.children.length === 0 && this.isRoot) {
        this.node.children.push(createRule());
      }
    },
    removeSelf() {
      this.$emit("remove");
    },
    syncRuleField(rule) {
      const fieldMeta = this.fieldMap[rule.field];
      if (!fieldMeta) {
        rule.operator = "";
        rule.nested_path = null;
        return;
      }
      rule.nested_path = fieldMeta.nested_path || null;
      if (!fieldMeta.operators.includes(rule.operator)) {
        rule.operator = fieldMeta.operators[0];
      }
      if (fieldMeta.field_type === "boolean" && rule.operator === "eq") {
        rule.value = true;
      }
    },
    operatorsForRule(rule) {
      const fieldMeta = this.fieldMap[rule.field];
      return fieldMeta ? fieldMeta.operators : [];
    },
    fieldType(rule) {
      const fieldMeta = this.fieldMap[rule.field];
      return fieldMeta ? fieldMeta.field_type : "";
    },
    fieldMeta(rule) {
      return this.fieldMap[rule.field] || null;
    },
    operatorLabel,
  },
  template: `
    <div :class="node.type === 'group' ? 'query-group' : 'query-rule'">
      <template v-if="node.type === 'group'">
        <div class="query-group-header">
          <div class="control-inline">
            <span class="control-label-inline">条件组逻辑</span>
            <help-tip :content="'选择当前条件组的组合方式。'"></help-tip>
          </div>
          <el-select v-model="node.logic" size="small" style="width: 160px">
            <el-option label="满足全部条件" value="and"></el-option>
            <el-option label="满足任一条件" value="or"></el-option>
          </el-select>
          <el-button size="small" type="primary" plain @click="addRule">新增条件</el-button>
          <el-button size="small" plain @click="addGroup">新增条件组</el-button>
          <el-button v-if="!isRoot" size="small" text type="danger" @click="removeSelf">删除条件组</el-button>
        </div>
        <div class="query-children">
          <query-builder
            v-for="(child, index) in node.children"
            :key="index"
            :node="child"
            :fields="fields"
            :field-map="fieldMap"
            :operator-labels="operatorLabels"
            :operator-help-text="operatorHelpText"
            @remove="removeChild(index)"
          ></query-builder>
        </div>
      </template>
      <template v-else>
        <div class="query-rule-grid">
          <div class="select-with-help">
            <el-select
              v-model="node.field"
              filterable
              clearable
              placeholder="选择字段"
              style="min-width: 240px"
              @change="syncRuleField(node)"
            >
              <el-option
                v-for="field in fields"
                :key="field.field_path"
                :label="field.cn_label + ' (' + field.field_path + ')'"
                :value="field.field_path"
              ></el-option>
            </el-select>
            <help-tip :content="'先选择字段，再选择该字段支持的中文筛选方式。'"></help-tip>
          </div>

          <div class="select-with-help">
            <el-select
              v-model="node.operator"
              placeholder="选择筛选方式"
              style="width: 180px"
              :disabled="!node.field"
            >
              <el-option
                v-for="operator in operatorsForRule(node)"
                :key="operator"
                :label="operatorLabel(operator)"
                :value="operator"
              ></el-option>
            </el-select>
            <help-tip :content="operatorHelpText[node.operator] || '根据字段类型选择适合的中文筛选方式。'"></help-tip>
          </div>

          <template v-if="node.operator === 'in' || node.operator === 'not_in'">
            <el-input
              v-model="node.valuesText"
              type="textarea"
              :rows="2"
              placeholder="多个值可用逗号、分号或换行分隔"
              style="min-width: 260px"
            ></el-input>
          </template>

          <template v-else-if="node.operator === 'between'">
            <el-input
              v-model="node.range.from"
              placeholder="起始值"
              style="width: 160px"
            ></el-input>
            <el-input
              v-model="node.range.to"
              placeholder="结束值"
              style="width: 160px"
            ></el-input>
          </template>

          <template v-else-if="node.operator !== 'exists' && node.operator !== 'not_exists'">
            <el-select
              v-if="fieldType(node) === 'boolean'"
              v-model="node.value"
              style="width: 140px"
            >
              <el-option label="是" :value="true"></el-option>
              <el-option label="否" :value="false"></el-option>
            </el-select>
            <el-input
              v-else
              v-model="node.value"
              :placeholder="fieldMeta(node)?.cn_label || '请输入值'"
              style="min-width: 220px"
            ></el-input>
          </template>

          <span v-if="fieldMeta(node)?.nested_path" class="table-tag">
            嵌套路径: {{ fieldMeta(node).nested_path }}
          </span>
          <el-button size="small" text type="danger" @click="removeSelf">删除</el-button>
        </div>
      </template>
    </div>
  `,
};

createApp({
  components: { HelpTip, QueryBuilder },
  setup() {
    const entities = ref([]);
    const selectedEntity = ref("");
    const entityMeta = ref(null);
    const fields = ref([]);
    const fieldMap = reactive({});
    const queryTree = reactive(createGroup());
    const selectedColumns = ref([]);
    const sortField = ref("");
    const sortOrder = ref("asc");
    const page = ref(1);
    const pageSize = ref(20);
    const total = ref(0);
    const rows = ref([]);
    const columns = ref([]);
    const generatedDsl = ref("");
    const generatedSqlRequest = ref("");
    const generatedSql = ref("");
    const warnings = ref([]);
    const countValue = ref(null);
    const cachedCountValue = ref(null);
    const loading = ref(false);
    const exportLoading = ref(false);
    const exportJob = ref(null);
    const sceneName = ref("");
    const selectedSceneId = ref("");
    const persistedState = ref(normalizePersistedState(readStorage(STORAGE_KEYS.lastState, createDefaultPersistedState())));
    const scenes = ref(normalizeScenes(readStorage(STORAGE_KEYS.scenes, [])));

    let suppressPersistence = false;
    let persistTimer = null;

    const currentEntityScenes = computed(() =>
      scenes.value.filter((scene) => scene.entity === selectedEntity.value)
    );

    const selectedScene = computed(() =>
      currentEntityScenes.value.find((scene) => scene.id === selectedSceneId.value) || null
    );

    const countLabel = computed(() => {
      if (countValue.value !== null) {
        return `${countValue.value} 条`;
      }
      if (cachedCountValue.value !== null) {
        return `上次统计 ${cachedCountValue.value} 条`;
      }
      return "未统计";
    });

    function clearResultState() {
      rows.value = [];
      columns.value = [];
      total.value = 0;
      generatedDsl.value = "";
      generatedSql.value = "";
      generatedSqlRequest.value = "";
      warnings.value = [];
      exportJob.value = null;
      countValue.value = null;
    }

    function persistScenes() {
      writeStorage(STORAGE_KEYS.scenes, scenes.value);
    }

    function buildCurrentEntityState() {
      const existingLastCount =
        persistedState.value.entities[selectedEntity.value]?.lastCountValue ?? cachedCountValue.value ?? null;

      return {
        queryTree: cloneData(queryTree),
        selectedColumns: [...selectedColumns.value],
        sortField: sortField.value,
        sortOrder: sortOrder.value,
        pageSize: pageSize.value,
        lastCountValue: countValue.value !== null ? countValue.value : existingLastCount,
      };
    }

    function persistCurrentState() {
      if (!selectedEntity.value || !entityMeta.value) {
        return;
      }
      persistedState.value.selectedEntity = selectedEntity.value;
      persistedState.value.entities[selectedEntity.value] = buildCurrentEntityState();
      writeStorage(STORAGE_KEYS.lastState, persistedState.value);
    }

    function schedulePersist() {
      if (suppressPersistence || !selectedEntity.value || !entityMeta.value) {
        return;
      }
      if (persistTimer) {
        window.clearTimeout(persistTimer);
      }
      persistTimer = window.setTimeout(() => {
        persistCurrentState();
      }, 180);
    }

    function applyEntityState(nextState, currentEntityMeta) {
      const defaultState = createDefaultEntityState(currentEntityMeta);
      const sanitizedColumns = Array.isArray(nextState?.selectedColumns)
        ? nextState.selectedColumns.filter((fieldName) => Boolean(fieldMap[fieldName]))
        : defaultState.selectedColumns;

      const nextSortField =
        typeof nextState?.sortField === "string" && fieldMap[nextState.sortField]?.sortable
          ? nextState.sortField
          : defaultState.sortField;
      const nextSortOrder = nextState?.sortOrder === "desc" ? "desc" : defaultState.sortOrder;
      const nextPageSize =
        Number.isInteger(nextState?.pageSize) && nextState.pageSize > 0 ? nextState.pageSize : defaultState.pageSize;

      selectedColumns.value = sanitizedColumns.length > 0 ? sanitizedColumns : defaultState.selectedColumns;
      sortField.value = nextSortField;
      sortOrder.value = nextSortOrder;
      pageSize.value = nextPageSize;
      cachedCountValue.value =
        typeof nextState?.lastCountValue === "number" ? nextState.lastCountValue : null;
      applyQueryTree(queryTree, sanitizeQueryTree(nextState?.queryTree || createGroup(), fieldMap));
    }

    async function api(url, options = {}) {
      const response = await fetch(url, {
        headers: { "Content-Type": "application/json" },
        ...options,
      });
      if (!response.ok) {
        const payload = await response.json().catch(() => ({}));
        throw new Error(payload.detail || "请求失败");
      }
      return response.json();
    }

    function buildRequestPayload() {
      const normalizedQueryTree = nodeForRequest(queryTree, fieldMap) || {
        type: "group",
        logic: "and",
        children: [],
      };
      return {
        entity: selectedEntity.value,
        query_tree: normalizedQueryTree,
        select_fields: selectedColumns.value,
        sort: sortField.value ? [{ field: sortField.value, order: sortOrder.value }] : [],
        page: page.value,
        page_size: pageSize.value,
      };
    }

    async function loadFields(options = {}) {
      if (!selectedEntity.value) {
        return;
      }

      suppressPersistence = true;
      const payload = await api(`/api/search/entities/${selectedEntity.value}/fields`);
      entityMeta.value = payload;
      fields.value = payload.fields || [];
      Object.keys(fieldMap).forEach((key) => delete fieldMap[key]);
      fields.value.forEach((field) => {
        fieldMap[field.field_path] = field;
      });

      const storedEntityState = persistedState.value.entities[selectedEntity.value];
      const nextState = options.forceDefault
        ? createDefaultEntityState(payload)
        : storedEntityState || createDefaultEntityState(payload);

      page.value = 1;
      clearResultState();
      applyEntityState(nextState, payload);

      selectedSceneId.value = "";
      sceneName.value = "";
      suppressPersistence = false;
      schedulePersist();
    }

    async function loadEntities() {
      const payload = await api("/api/search/entities");
      entities.value = payload.entities || [];
      const entityKeys = new Set(entities.value.map((item) => item.entity_key));
      const preferredEntity = persistedState.value.selectedEntity;
      selectedEntity.value =
        preferredEntity && entityKeys.has(preferredEntity)
          ? preferredEntity
          : entities.value[0]?.entity_key || "";
      await loadFields();
    }

    async function handleEntityChange() {
      await loadFields();
    }

    async function runQuery() {
      loading.value = true;
      try {
        const payload = await api("/api/search/query", {
          method: "POST",
          body: JSON.stringify(buildRequestPayload()),
        });
        rows.value = payload.rows || [];
        columns.value = payload.columns || [];
        total.value = payload.total || 0;
        generatedDsl.value = payload.generated_dsl_pretty || "";
        generatedSql.value = payload.generated_sql || "";
        generatedSqlRequest.value = payload.generated_sql_request_pretty || "";
        warnings.value = payload.warnings || [];
      } catch (error) {
        ElementPlus.ElMessage.error(error.message);
      } finally {
        loading.value = false;
      }
    }

    async function runCount() {
      try {
        const payload = await api("/api/search/count", {
          method: "POST",
          body: JSON.stringify({
            entity: selectedEntity.value,
            query_tree: nodeForRequest(queryTree, fieldMap) || {
              type: "group",
              logic: "and",
              children: [],
            },
          }),
        });
        countValue.value = payload.total || 0;
        cachedCountValue.value = countValue.value;
        warnings.value = payload.warnings || [];
        schedulePersist();
        ElementPlus.ElMessage.success(`当前条件命中 ${countValue.value} 条`);
      } catch (error) {
        ElementPlus.ElMessage.error(error.message);
      }
    }

    async function runExport(formatName) {
      exportLoading.value = true;
      try {
        const payload = await api("/api/search/export", {
          method: "POST",
          body: JSON.stringify({
            entity: selectedEntity.value,
            query_tree: nodeForRequest(queryTree, fieldMap) || {
              type: "group",
              logic: "and",
              children: [],
            },
            select_fields: selectedColumns.value,
            sort: sortField.value ? [{ field: sortField.value, order: sortOrder.value }] : [],
            format: formatName,
          }),
        });
        exportJob.value = payload.job;
        await pollExport(payload.job.job_id);
      } catch (error) {
        ElementPlus.ElMessage.error(error.message);
      } finally {
        exportLoading.value = false;
      }
    }

    async function pollExport(jobId) {
      for (let attempt = 0; attempt < 40; attempt += 1) {
        const payload = await api(`/api/search/export/jobs/${jobId}`);
        exportJob.value = payload.job;
        if (payload.job.status === "completed") {
          window.open(payload.job.download_path, "_blank");
          ElementPlus.ElMessage.success(`导出完成，共 ${payload.job.row_count} 条`);
          return;
        }
        if (payload.job.status === "failed") {
          throw new Error(payload.job.error || "导出失败");
        }
        await new Promise((resolve) => window.setTimeout(resolve, 1500));
      }
      ElementPlus.ElMessage.warning("导出仍在后台执行，可稍后刷新状态。");
    }

    function resetAll() {
      if (selectedEntity.value && persistedState.value.entities[selectedEntity.value]) {
        delete persistedState.value.entities[selectedEntity.value];
        persistedState.value.selectedEntity = selectedEntity.value;
        writeStorage(STORAGE_KEYS.lastState, persistedState.value);
      }
      loadFields({ forceDefault: true });
      ElementPlus.ElMessage.success("已恢复当前实体的默认条件与默认展示。");
    }

    function clearSelectedColumns() {
      selectedColumns.value = [];
      ElementPlus.ElMessage.success("结果列选择已清空。");
    }

    function saveScene() {
      const nextName = sceneName.value.trim();
      if (!nextName) {
        ElementPlus.ElMessage.warning("请先输入场景名称。");
        return;
      }

      const nextScene = createSceneFromState(selectedEntity.value, buildCurrentEntityState(), nextName);
      scenes.value = [...scenes.value, nextScene];
      persistScenes();
      selectedSceneId.value = nextScene.id;
      sceneName.value = nextScene.name;
      ElementPlus.ElMessage.success("场景已保存到当前浏览器。");
    }

    function overwriteScene() {
      if (!selectedScene.value) {
        ElementPlus.ElMessage.warning("请先选择要覆盖的场景。");
        return;
      }

      const nextName = sceneName.value.trim() || selectedScene.value.name;
      scenes.value = scenes.value.map((scene) =>
        scene.id === selectedScene.value.id
          ? {
              ...scene,
              name: nextName,
              queryTree: cloneData(queryTree),
              selectedColumns: [...selectedColumns.value],
              sortField: sortField.value,
              sortOrder: sortOrder.value,
              pageSize: pageSize.value,
              updatedAt: new Date().toISOString(),
            }
          : scene
      );
      persistScenes();
      sceneName.value = nextName;
      ElementPlus.ElMessage.success("已覆盖所选场景。");
    }

    function applyScene() {
      if (!selectedScene.value) {
        ElementPlus.ElMessage.warning("请先选择一个场景。");
        return;
      }

      suppressPersistence = true;
      applyEntityState(selectedScene.value, entityMeta.value);
      clearResultState();
      page.value = 1;
      suppressPersistence = false;
      schedulePersist();
      ElementPlus.ElMessage.success("场景已恢复，请按需执行查询。");
    }

    function deleteScene() {
      if (!selectedScene.value) {
        ElementPlus.ElMessage.warning("请先选择要删除的场景。");
        return;
      }

      const nextSceneId = selectedScene.value.id;
      scenes.value = scenes.value.filter((scene) => scene.id !== nextSceneId);
      persistScenes();
      selectedSceneId.value = "";
      sceneName.value = "";
      ElementPlus.ElMessage.success("场景已删除。");
    }

    watch(
      selectedSceneId,
      (nextSceneId) => {
        if (!nextSceneId) {
          return;
        }
        const scene = currentEntityScenes.value.find((item) => item.id === nextSceneId);
        if (scene) {
          sceneName.value = scene.name;
        }
      },
      { immediate: false }
    );

    watch(selectedColumns, schedulePersist, { deep: true });
    watch(sortField, schedulePersist);
    watch(sortOrder, schedulePersist);
    watch(pageSize, schedulePersist);
    watch(queryTree, schedulePersist, { deep: true });

    onMounted(loadEntities);

    return {
      HELP_TEXT,
      OPERATOR_LABELS,
      OPERATOR_HELP_TEXT,
      entities,
      selectedEntity,
      entityMeta,
      fields,
      fieldMap,
      queryTree,
      selectedColumns,
      sortField,
      sortOrder,
      page,
      pageSize,
      total,
      rows,
      columns,
      generatedDsl,
      generatedSqlRequest,
      generatedSql,
      warnings,
      countLabel,
      loading,
      exportLoading,
      exportJob,
      sceneName,
      currentEntityScenes,
      selectedSceneId,
      handleEntityChange,
      runQuery,
      runCount,
      resetAll,
      clearSelectedColumns,
      runExport,
      saveScene,
      overwriteScene,
      applyScene,
      deleteScene,
      formatCell,
      operatorLabel,
    };
  },
  template: `
    <div class="page-shell">
      <div class="page-grid">
        <main class="main-column">
          <section class="panel-card">
            <div class="panel-header">
              <div class="title-with-help">
                <h2 class="panel-title">常用场景</h2>
                <help-tip :content="HELP_TEXT.scenes"></help-tip>
              </div>
              <span class="hint-text">场景只保存在当前浏览器，不会同步到其他设备。</span>
            </div>

            <div class="scene-grid">
              <div class="control-stack">
                <div class="control-label">
                  场景名称
                  <help-tip content="输入名称后可把当前实体、筛选、结果列和排序保存为一个场景。"></help-tip>
                </div>
                <el-input v-model="sceneName" placeholder="例如：高被引免疫学论文"></el-input>
              </div>

              <div class="control-stack">
                <div class="control-label">
                  当前实体场景
                  <help-tip content="这里只显示当前实体下保存过的场景。"></help-tip>
                </div>
                <el-select
                  v-model="selectedSceneId"
                  filterable
                  clearable
                  placeholder="选择已保存场景"
                >
                  <el-option
                    v-for="scene in currentEntityScenes"
                    :key="scene.id"
                    :label="scene.name"
                    :value="scene.id"
                  ></el-option>
                </el-select>
              </div>
            </div>

            <div class="toolbar-row">
              <el-button type="primary" plain @click="saveScene">保存为新场景</el-button>
              <el-button plain @click="overwriteScene">覆盖所选场景</el-button>
              <el-button plain @click="applyScene">应用所选场景</el-button>
              <el-button text type="danger" @click="deleteScene">删除所选场景</el-button>
              <span class="hint-text" v-if="currentEntityScenes.length === 0">当前实体还没有保存过场景。</span>
            </div>
          </section>

          <section class="panel-card">
            <div class="panel-header">
              <div class="title-with-help">
                <h2 class="panel-title">实体与字段</h2>
                <help-tip :content="HELP_TEXT.entity"></help-tip>
              </div>
              <span class="hint-text">字段目录来自 schema + mapping，不在前端硬编码。</span>
            </div>

            <div class="toolbar-row">
              <div class="control-stack control-stack-sm">
                <div class="control-label">
                  实体
                  <help-tip :content="HELP_TEXT.entity"></help-tip>
                </div>
                <el-select v-model="selectedEntity" style="width: 220px" @change="handleEntityChange">
                  <el-option
                    v-for="entity in entities"
                    :key="entity.entity_key"
                    :label="entity.cn_label + ' (' + entity.entity_key + ')'"
                    :value="entity.entity_key"
                  ></el-option>
                </el-select>
              </div>

              <div class="control-stack control-stack-lg">
                <div class="control-label">
                  结果列
                  <help-tip :content="HELP_TEXT.columns"></help-tip>
                </div>
                <div class="select-with-help">
                  <el-select
                    v-model="selectedColumns"
                    multiple
                    filterable
                    collapse-tags
                    collapse-tags-tooltip
                    style="min-width: 420px; flex: 1"
                    placeholder="选择结果列"
                  >
                    <el-option
                      v-for="field in fields"
                      :key="field.field_path"
                      :label="field.cn_label + ' (' + field.field_path + ')'"
                      :value="field.field_path"
                    ></el-option>
                  </el-select>
                  <el-button plain @click="clearSelectedColumns">清空结果列</el-button>
                </div>
              </div>

              <div class="control-stack control-stack-sm">
                <div class="control-label">
                  排序字段
                  <help-tip :content="HELP_TEXT.sortField"></help-tip>
                </div>
                <el-select v-model="sortField" filterable clearable style="width: 220px" placeholder="排序字段">
                  <el-option
                    v-for="field in fields.filter((item) => item.sortable)"
                    :key="field.field_path"
                    :label="field.cn_label + ' (' + field.field_path + ')'"
                    :value="field.field_path"
                  ></el-option>
                </el-select>
              </div>

              <div class="control-stack control-stack-xs">
                <div class="control-label">
                  排序方向
                  <help-tip :content="HELP_TEXT.sortOrder"></help-tip>
                </div>
                <el-select v-model="sortOrder" style="width: 120px">
                  <el-option label="升序" value="asc"></el-option>
                  <el-option label="降序" value="desc"></el-option>
                </el-select>
              </div>
            </div>
          </section>

          <section class="panel-card">
            <div class="panel-header">
              <div class="title-with-help">
                <h2 class="panel-title">结构化筛选</h2>
                <help-tip :content="HELP_TEXT.logic"></help-tip>
              </div>
              <span class="hint-text">支持任意字段组合筛选，同一 nested 路径会自动归并。</span>
            </div>
            <query-builder
              :node="queryTree"
              :fields="fields"
              :field-map="fieldMap"
              :operator-labels="OPERATOR_LABELS"
              :operator-help-text="OPERATOR_HELP_TEXT"
              :is-root="true"
            ></query-builder>

            <div class="toolbar-row" style="margin-top: 16px;">
              <el-button type="primary" :loading="loading" @click="runQuery">执行查询</el-button>
              <el-button @click="runCount">仅统计数量</el-button>
              <el-button @click="resetAll">恢复默认</el-button>
              <el-button :loading="exportLoading" @click="runExport('csv')">导出 CSV</el-button>
              <el-button :loading="exportLoading" @click="runExport('xlsx')">导出 XLSX</el-button>

              <div class="control-inline">
                <span class="control-label-inline">页码</span>
                <el-input-number v-model="page" :min="1" label="页码"></el-input-number>
              </div>
              <div class="control-inline">
                <span class="control-label-inline">每页条数</span>
                <el-input-number v-model="pageSize" :min="1" :max="200" label="每页条数"></el-input-number>
              </div>
            </div>

            <div class="warning-list" v-if="warnings.length">
              <el-alert
                v-for="(warning, index) in warnings"
                :key="index"
                type="warning"
                :closable="false"
                :title="warning"
              ></el-alert>
            </div>
          </section>

          <section class="panel-card">
            <div class="panel-header">
              <h2 class="panel-title">结果表</h2>
              <div class="result-toolbar">
                <span class="hint-text">总计 {{ total }} 条</span>
              </div>
            </div>

            <el-table :data="rows" border stripe v-loading="loading" style="width: 100%">
              <el-table-column
                v-for="column in columns"
                :key="column.field_path"
                :prop="column.field_path"
                :label="column.cn_label"
                min-width="180"
                show-overflow-tooltip
              >
                <template #default="scope">
                  {{ formatCell(scope.row[column.field_path], column) }}
                </template>
              </el-table-column>
            </el-table>
          </section>

          <section class="panel-card">
            <div class="panel-header">
              <div class="title-with-help">
                <h2 class="panel-title">只读 DSL / SQL 预览</h2>
                <help-tip :content="HELP_TEXT.preview"></help-tip>
              </div>
              <span class="hint-text">DSL 为实际执行真源，SQL 为预览和调试辅助。</span>
            </div>

            <el-tabs>
              <el-tab-pane label="OpenSearch DSL">
                <pre class="code-panel">{{ generatedDsl }}</pre>
              </el-tab-pane>
              <el-tab-pane label="SQL">
                <pre class="code-panel">{{ generatedSql || '--' }}</pre>
              </el-tab-pane>
              <el-tab-pane label="SQL Request">
                <pre class="code-panel">{{ generatedSqlRequest }}</pre>
              </el-tab-pane>
            </el-tabs>
          </section>
        </main>
      </div>
    </div>
  `,
}).use(ElementPlus).mount("#app");
