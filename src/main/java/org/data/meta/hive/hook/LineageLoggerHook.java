package org.data.meta.hive.hook;


import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.SetUtils;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.LineageInfo;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.lineage.LineageCtx;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.util.StringUtils;
import org.data.meta.hive.model.event.EventBase;
import org.data.meta.hive.model.lineage.ColumnLineage;
import org.data.meta.hive.model.lineage.Edge;
import org.data.meta.hive.model.lineage.LineageHookInfo;
import org.data.meta.hive.model.lineage.LineageTable;
import org.data.meta.hive.model.lineage.LineageTableColumn;
import org.data.meta.hive.model.lineage.TableLineage;
import org.data.meta.hive.model.lineage.Vertex;
import org.data.meta.hive.service.emitter.EventEmitterFactory;
import org.data.meta.hive.util.EventUtils;
import org.data.meta.hive.util.MetaLogUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.data.meta.hive.util.JsonUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 自定义Hook解析字段级血缘关系，输出到指定到文件中
 * 配置在hive.exec.post.hooks，在执行计划之后会执行这个钩子函数解析血缘关系
 * 可参考 官方到Demo org.apache.hadoop.hive.ql.hooks.LineageLogger
 */
public class LineageLoggerHook implements ExecuteWithHookContext {
    private static final String FORMAT_VERSION = "1.0";
    private static final HashSet<String> OPERATION_NAMES = new HashSet<>();

    private static final Logger LOG = LoggerFactory.getLogger(LineageLoggerHook.class);

    //目前只监控这几个操作，官方源码解析就这几个，如果特殊需要可以增加
    static {
        LineageLoggerHook.OPERATION_NAMES.add(HiveOperation.QUERY.getOperationName());
        LineageLoggerHook.OPERATION_NAMES.add(HiveOperation.CREATETABLE_AS_SELECT.getOperationName());
        LineageLoggerHook.OPERATION_NAMES.add(HiveOperation.ALTERVIEW_AS.getOperationName());
        LineageLoggerHook.OPERATION_NAMES.add(HiveOperation.CREATEVIEW.getOperationName());
    }

    @Override
    public void run(final HookContext hookContext) {
        //和配置文件相对应
        assert hookContext.getHookType() == HookContext.HookType.POST_EXEC_HOOK;
        //执行计划
        final QueryPlan plan = hookContext.getQueryPlan();
        //org.apache.hadoop.hive.ql.optimizer.lineage.LineageCtx,血缘的上下文
        final LineageCtx.Index index = hookContext.getIndex();
        final SessionState ss = SessionState.get();
        if (ss != null && index != null && LineageLoggerHook.OPERATION_NAMES.contains(plan.getOperationName()) && !plan.isExplain()) {
            try {
                //获取信息
                String version = null;
                //执行用户
                String user = null;
                String[] userGroupNames = null;
                Long timestamp = null;
                long duration = 0L;
                //TODO 看看怎么ID可以从哪里获取
                final List<String> jobIds = new ArrayList<>();
                //执行引擎
                String engine = null;
                String database = null;
                //HQL哈希值
                String hash = null;
                //HQL
                String queryText = null;
                final String queryStr = plan.getQueryStr().trim();
                version = "1.0";
                final HiveConf conf = ss.getConf();
                long queryTime = plan.getQueryStartTime();
                if (queryTime == 0L) {
                    queryTime = System.currentTimeMillis();
                }
                duration = System.currentTimeMillis() - queryTime;
                //TODO 这个UGI是什么意思？在hadoop审计日志也有这个UGI，这个UGI还自带执行用户信息
                //RetryingMetaStoreClient proxy=class org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient ugi=hive/miniso-newpt2@MINISO-BDP-TEST.CN (auth:KERBEROS) retries=24 delay=5 lifetime=0
                user = hookContext.getUgi().getUserName();
                userGroupNames = hookContext.getUgi().getGroupNames();
                timestamp = queryTime / 1000L;
                //TODO 这里可以拿到JobID，是不是任务初始化过程那里已经有的，在hook函数执行的时候已经传给Hook了
                final List<TaskRunner> tasks = hookContext.getCompleteTaskList();
                if (tasks != null && !tasks.isEmpty()) {
                    for (final TaskRunner task : tasks) {
                        final String jobId = task.getTask().getJobID();
                        if (jobId != null) {
                            jobIds.add(jobId);
                        }
                    }
                }
                //所以这个配置文件里面就有的，实际可以任务执行中来覆盖这个值
                engine = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE);
                database = MetaLogUtils.normalizeIdentifier(ss.getCurrentDatabase());
                hash = DigestUtils.md5Hex(queryStr);
                queryText = queryStr;
                //TODO 我估摸着这个解法和官方处理最后得到edge是差不多的逻辑
                final List<Edge> edges = this.getEdges(plan, index);
                //根据edge获取表级血缘关系
                final List<TableLineage> tableLineages = this.buildTableLineages(edges);
                //根据edge获取字段级血缘关系
                final List<ColumnLineage> columnLineages = this.buildColumnLineages(edges);

                //消息设置
                final LineageHookInfo lhInfo = new LineageHookInfo();
                lhInfo.setConf(hookContext.getConf().get("dw_output"));
                lhInfo.setDatabase(database);
                lhInfo.setDuration(duration);
                lhInfo.setEngine(engine);
                lhInfo.setHash(hash);
                lhInfo.setJobIds(jobIds);
                lhInfo.setQueryText(queryText);
                lhInfo.setTimestamp(timestamp);
                lhInfo.setUser(user);
                lhInfo.setUserGroupNames(userGroupNames);
                lhInfo.setVersion(version);
                lhInfo.setTableLineages(tableLineages);
                lhInfo.setColumnLineages(columnLineages);

                //提交事件
                final EventBase<LineageHookInfo> event = new EventBase<>();
                event.setEventType("LINEAGE");
                event.setContent(lhInfo);
                event.setId(EventUtils.newId());
                event.setTimestamp(System.currentTimeMillis());
                event.setType("HIVE");
                EventEmitterFactory.get().emit(event);
            } catch (Throwable t) {
                this.log("Failed to log lineage graph, query is not affected\n" + StringUtils.stringifyException(t));
            }
        }
    }

    /**
     * 解析原来的血缘
     *
     * @param plan  执行计划
     * @param index org.apache.hadoop.hive.ql.optimizer.lineage.LineageCtx,血缘的上下文
     * @return
     */
    private List<Edge> getEdges(final QueryPlan plan, final LineageCtx.Index index) {
        final LinkedHashMap<String, ObjectPair<SelectOperator, Table>> finalSelOps = index.getFinalSelectOps();
        final Map<String, Vertex> vertexCache = new LinkedHashMap<>();
        final List<Edge> edges = new ArrayList<>();
        for (final ObjectPair<SelectOperator, Table> pair : finalSelOps.values()) {
            List<FieldSchema> fieldSchemas = plan.getResultSchema().getFieldSchemas();
            final SelectOperator finalSelOp = pair.getFirst();
            Table t = pair.getSecond();
            String destPureDbName = null;
            String destPureTableName = null;
            String destTableName = null;
            List<String> colNames = null;
            if (t != null) {
                destPureDbName = t.getDbName();
                destPureTableName = t.getTableName();
                destTableName = t.getDbName() + "." + t.getTableName();
                fieldSchemas = t.getCols();
            } else {
                for (final WriteEntity output : plan.getOutputs()) {
                    final Entity.Type entityType = output.getType();
                    if (entityType == Entity.Type.TABLE || entityType == Entity.Type.PARTITION) {
                        t = output.getTable();
                        destPureDbName = t.getDbName();
                        destPureTableName = t.getTableName();
                        destTableName = t.getDbName() + "." + t.getTableName();
                        final List<FieldSchema> cols = t.getCols();
                        if (cols != null && !cols.isEmpty()) {
                            colNames = (List<String>) Utilities.getColumnNamesFromFieldSchema((List) cols);
                            break;
                        }
                        break;
                    }
                }
            }
            final Map<ColumnInfo, LineageInfo.Dependency> colMap = (Map<ColumnInfo, LineageInfo.Dependency>) index.getDependencies((Operator) finalSelOp);
            final List<LineageInfo.Dependency> dependencies = (colMap != null) ? new ArrayList<>(colMap.values()) : null;
            int fields = fieldSchemas.size();
            if (t != null && colMap != null && fields < colMap.size()) {
                final List<FieldSchema> partitionKeys = t.getPartitionKeys();
                final int dynamicKeyCount = colMap.size() - fields;
                final int keyOffset = partitionKeys.size() - dynamicKeyCount;
                if (keyOffset >= 0) {
                    fields += dynamicKeyCount;
                    for (int i = 0; i < dynamicKeyCount; ++i) {
                        final FieldSchema field = partitionKeys.get(keyOffset + i);
                        fieldSchemas.add(field);
                        if (colNames != null) {
                            colNames.add(field.getName());
                        }
                    }
                }
            }
            if (dependencies == null || dependencies.size() != fields) {
                this.log("Result schema has " + fields + " fields, but we don't get as many dependencies");
            } else {
                final Set<Vertex> targets = new LinkedHashSet<>();
                for (int j = 0; j < fields; ++j) {
                    final Vertex target = this.getOrCreateVertex(vertexCache, this.getTargetFieldName(j, destTableName, colNames, fieldSchemas), Vertex.Type.COLUMN, destPureDbName, destPureTableName, this.getTargetPureFieldName(j, colNames, fieldSchemas));
                    targets.add(target);
                    final LineageInfo.Dependency dep = dependencies.get(j);
                    this.addEdge(vertexCache, edges, dep.getBaseCols(), target, dep.getExpr(), Edge.Type.PROJECTION);
                }
                final Set<LineageInfo.Predicate> conds = (Set<LineageInfo.Predicate>) index.getPredicates((Operator) finalSelOp);
                if (conds == null || conds.isEmpty()) {
                    continue;
                }
                for (final LineageInfo.Predicate cond : conds) {
                    this.addEdge(vertexCache, edges, cond.getBaseCols(), new LinkedHashSet<>(targets), cond.getExpr(), Edge.Type.PREDICATE);
                }
            }
        }
        return edges;
    }

    /**
     * 表级血缘
     *
     * @param edges edge
     * @return
     */
    private List<TableLineage> buildTableLineages(final List<Edge> edges) {
        final Set<TableLineage> tableLineages = new HashSet<>();
        for (final Edge edge : edges) {
            final List<LineageTable> sources = new ArrayList<>();
            for (final Vertex vertex : edge.sources) {
                final String srcDatabase = MetaLogUtils.normalizeIdentifier(vertex.dbName);
                final String srcTable = MetaLogUtils.normalizeIdentifier(vertex.tableName);
                sources.add(new LineageTable(srcDatabase, srcTable));
            }
            final List<LineageTable> targets = new ArrayList<>();
            for (final Vertex vertex2 : edge.targets) {
                final String destDatabase = MetaLogUtils.normalizeIdentifier(vertex2.dbName);
                final String destTable = MetaLogUtils.normalizeIdentifier(vertex2.tableName);
                targets.add(new LineageTable(destDatabase, destTable));
            }
            for (final LineageTable source : sources) {
                for (final LineageTable target : targets) {
                    final String srcDatabase2 = source.getDatabase();
                    final String destDatabase2 = target.getDatabase();
                    final String srcTable2 = source.getTable();
                    final String destTable2 = target.getTable();
                    final TableLineage tableLineage = new TableLineage();
                    tableLineage.setSrcTable(srcDatabase2 + "." + srcTable2);
                    tableLineage.setDestTable(destDatabase2 + "." + destTable2);
                    //不为空才添加
                    if (destDatabase2 != null && destTable2 != null) {
                        tableLineages.add(tableLineage);
                    }
                }
            }
        }
        return new ArrayList<>(tableLineages);
    }

    /**
     * 字段级血缘关系
     *
     * @param edges edge
     * @return
     */
    private List<ColumnLineage> buildColumnLineages(final List<Edge> edges) {
        final List<ColumnLineage> columnLineages = new ArrayList<>();
        for (final Edge edge : edges) {
            String srcDatabase = null;
            String destDatabase = null;
            String expression = null;
            final Edge.Type edgeType = edge.type;
            final List<LineageTableColumn> sources = new ArrayList<>();
            for (final Vertex vertex : edge.sources) {
                srcDatabase = MetaLogUtils.normalizeIdentifier(vertex.dbName);
                final String srcTableName = MetaLogUtils.normalizeIdentifier(vertex.tableName);
                sources.add(new LineageTableColumn(srcTableName, vertex.columnName));
            }
            final List<LineageTableColumn> targets = new ArrayList<>();
            for (final Vertex vertex2 : edge.targets) {
                destDatabase = vertex2.dbName;
                final String destTableName = MetaLogUtils.normalizeIdentifier(vertex2.tableName);
                targets.add(new LineageTableColumn(destTableName, vertex2.columnName));
            }
            if (edge.expr != null) {
                expression = edge.expr;
            }
            final ColumnLineage columnLineage = new ColumnLineage();
            columnLineage.setEdgeType(edgeType);
            columnLineage.setExpression(expression);
            columnLineage.setSrcDatabase(srcDatabase);
            columnLineage.setDestDatabase(destDatabase);
            columnLineage.setSources(sources);
            columnLineage.setTargets(targets);
            columnLineages.add(columnLineage);
        }
        return columnLineages;
    }

    private void addEdge(final Map<String, Vertex> vertexCache, final List<Edge> edges, final Set<LineageInfo.BaseColumnInfo> srcCols, final Vertex target, final String expr, final Edge.Type type) {
        final Set<Vertex> targets = new LinkedHashSet<>();
        targets.add(target);
        this.addEdge(vertexCache, edges, srcCols, targets, expr, type);
    }

    private void addEdge(final Map<String, Vertex> vertexCache, final List<Edge> edges, final Set<LineageInfo.BaseColumnInfo> srcCols, final Set<Vertex> targets, final String expr, final Edge.Type type) {
        final Set<Vertex> sources = this.createSourceVertices(vertexCache, srcCols);
        final Edge edge = this.findSimilarEdgeBySources(edges, sources, expr, type);
        if (edge == null) {
            edges.add(new Edge(sources, targets, expr, type));
        } else {
            edge.targets.addAll(targets);
        }
    }

    private Set<Vertex> createSourceVertices(final Map<String, Vertex> vertexCache, final Collection<LineageInfo.BaseColumnInfo> baseCols) {
        final Set<Vertex> sources = new LinkedHashSet<>();
        if (baseCols != null && !baseCols.isEmpty()) {
            for (final LineageInfo.BaseColumnInfo col : baseCols) {
                final org.apache.hadoop.hive.metastore.api.Table table = col.getTabAlias().getTable();
                if (table.isTemporary()) {
                    continue;
                }
                Vertex.Type type = Vertex.Type.TABLE;
                final String fullTableName = table.getDbName() + "." + table.getTableName();
                final FieldSchema fieldSchema = col.getColumn();
                String label = fullTableName;
                final String dbName = table.getDbName();
                final String tableName = table.getTableName();
                String columnName = null;
                if (fieldSchema != null) {
                    type = Vertex.Type.COLUMN;
                    label = fullTableName + "." + fieldSchema.getName();
                    columnName = fieldSchema.getName();
                }
                sources.add(this.getOrCreateVertex(vertexCache, label, type, dbName, tableName, columnName));
            }
        }
        return sources;
    }

    private Edge findSimilarEdgeBySources(final List<Edge> edges, final Set<Vertex> sources, final String expr, final Edge.Type type) {
        for (final Edge edge : edges) {
            if (edge.type == type && org.apache.commons.lang.StringUtils.equals(edge.expr, expr) && SetUtils.isEqualSet((Collection) edge.sources, (Collection) sources)) {
                return edge;
            }
        }
        return null;
    }

    private String getTargetFieldName(final int fieldIndex, final String destTableName, final List<String> colNames, final List<FieldSchema> fieldSchemas) {
        final String fieldName = fieldSchemas.get(fieldIndex).getName();
        final String[] parts = fieldName.split("\\.");
        if (destTableName != null) {
            String colName = parts[parts.length - 1];
            if (colNames != null && !colNames.contains(colName)) {
                colName = colNames.get(fieldIndex);
            }
            return destTableName + "." + colName;
        }
        if (parts.length == 2 && parts[0].startsWith("_u")) {
            return parts[1];
        }
        return fieldName;
    }

    private String getTargetPureFieldName(final int fieldIndex, final List<String> colNames, final List<FieldSchema> fieldSchemas) {
        final String fieldName = fieldSchemas.get(fieldIndex).getName();
        final String[] parts = fieldName.split("\\.");
        final String colName = parts[parts.length - 1];
        if (org.apache.commons.lang.StringUtils.isNotEmpty(colName)) {
            return colName;
        }
        if (parts.length == 2 && parts[0].startsWith("_u")) {
            return parts[1];
        }
        return fieldName;
    }

    private Vertex getOrCreateVertex(final Map<String, Vertex> vertices, final String label, final Vertex.Type type, final String dbName, final String tableName, final String columnName) {
        Vertex vertex = vertices.get(label);
        if (vertex == null) {
            vertex = new Vertex(label, type, dbName, tableName, columnName);
            vertices.put(label, vertex);
        }
        return vertex;
    }

    private void log(final String error) {
        final SessionState.LogHelper console = SessionState.getConsole();
        if (console != null) {
            console.printError(error);
        }
    }
}
