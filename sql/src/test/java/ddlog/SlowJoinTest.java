package ddlog;

import com.vmware.ddlog.DDlogJooqProvider;
import com.vmware.ddlog.ir.DDlogProgram;
import com.vmware.ddlog.translator.Translator;
import com.vmware.ddlog.util.sql.*;
import ddlogapi.DDlogAPI;
import ddlogapi.DDlogException;
import io.fabric8.kubernetes.api.model.*;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Insert;
import org.jooq.Query;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SlowJoinTest {
    protected static DDlogAPI ddlogAPI;
    protected static DSLContext create;
    protected static DDlogJooqProvider provider;

    public static <R extends SqlStatement> DDlogAPI compileAndLoad(
            final List<R> ddl, final ToPrestoTranslator<R> translator,
            final List<String> createIndexStatements) throws IOException, DDlogException {
        final String fileName = "/tmp/program.dl";
        /*final Translator t = new Translator(null);
        ddl.forEach(x -> t.translateSqlStatement(translator.toPresto(x)));
        createIndexStatements.forEach(t::translateCreateIndexStatement);

        final DDlogProgram dDlogProgram = t.getDDlogProgram();
        final String fileName = "/tmp/program.dl";
        File tmp = new File(fileName);
        BufferedWriter bw = new BufferedWriter(new FileWriter(tmp));
        bw.write(dDlogProgram.toString());
        bw.close();*/
        DDlogAPI.CompilationResult result = new DDlogAPI.CompilationResult(true);
        DDlogAPI.compileDDlogProgram(fileName, result, "../lib", "./lib");
        if (!result.isSuccess())
            throw new RuntimeException("Failed to compile ddlog program");
        return DDlogAPI.loadDDlog();
    }

    @BeforeClass
    public static void setup() throws IOException, DDlogException {

        List<String> ddl = new ArrayList<>();
        ddl.add("create table node_info\n" +
                "                (" +
                "                uid varchar(36) not null," +
                "                name varchar(253) not null," +
                "                unschedulable boolean not null," +
                "                out_of_disk boolean not null," +
                "                memory_pressure boolean not null," +
                "                disk_pressure boolean not null," +
                "                pid_pressure boolean not null," +
                "                ready boolean not null," +
                "                network_unavailable boolean not null," +
                "                primary key(uid)" +
                "                )");
        ddl.add("create table pod_info\n" +
                "(\n" +
                "  uid varchar(36) not null,\n" +
                "  pod_name varchar(253) not null,\n" +
                "  status varchar(36) not null,\n" +
                "  node_name varchar(253) null,\n" +
                "  namespace varchar(253) not null,\n" +
                "  owner_name varchar(100) not null,\n" +
                "  creation_timestamp varchar(100) not null,\n" +
                "  priority integer not null,\n" +
                "  scheduler_name varchar(50),\n" +
                "  has_node_selector_labels boolean not null,\n" +
                "  has_pod_affinity_requirements boolean not null,\n" +
                "  has_pod_anti_affinity_requirements boolean not null,\n" +
                "  has_node_port_requirements boolean not null,\n" +
                "  has_topology_spread_constraints boolean not null,\n" +
                "  equivalence_class bigint not null,\n" +
                "  qos_class varchar(10) not null,\n" +
                "  resourceVersion bigint not null,\n" +
                "  last_requeue bigint not null,\n" +
                "  primary key(uid),\n" +
                "  constraint uc_namespaced_pod_name unique (pod_name, namespace)\n" +
                ")\n");
        ddl.add("create table node_resources\n" +
                "(\n" +
                "  uid varchar(36) not null,\n" +
                "  resource varchar(100) not null,\n" +
                "  allocatable bigint not null\n" +
                ")\n");
        ddl.add("create table pod_resource_demands\n" +
                "(\n" +
                "  uid varchar(36) not null,\n" +
                "  resource varchar(100) not null,\n" +
                "  demand bigint not null\n" +
                ")\n");

        // Now add the slow capacity view
        ddl.add("create view spare_capacity_per_node as " +
                "" +
                        "SELECT node_info.name AS name,\n" +
                        "                       node_resources.resource,\n" +
                        "                       node_resources.allocatable - CAST(sum(A.total_demand) as bigint) as capacity\n" +
                        "                FROM node_info\n" +
                        "                JOIN node_resources\n" +
                        "                    ON node_info.uid = node_resources.uid\n" +
                        "                LEFT JOIN (SELECT pod_info.node_name,\n" +
                        "                             pod_resource_demands.resource,\n" +
                        "                             sum(pod_resource_demands.demand) AS total_demand\n" +
                        "                     FROM pod_info\n" +
                        "                     JOIN pod_resource_demands\n" +
                        "                       ON pod_resource_demands.uid = pod_info.uid\n" +
                        "                     GROUP BY pod_info.node_name, pod_resource_demands.resource) A\n" +
                        "                    ON A.node_name = node_info.name AND A.resource = node_resources.resource\n" +
                        "                WHERE unschedulable = false AND\n" +
                        "                      memory_pressure = false AND\n" +
                        "                      out_of_disk = false AND\n" +
                        "                      disk_pressure = false AND\n" +
                        "                      pid_pressure = false AND\n" +
                        "                      network_unavailable = false AND\n" +
                        "                      ready = true\n" +
                        "                GROUP BY node_info.name, node_resources.resource, node_resources.allocatable"
                );


        List<String> indexStatements = new ArrayList<>();
        indexStatements.add("create index pod_info_idx on pod_info (status, node_name)");
        indexStatements.add("create index pod_resource_demands_by_uid on pod_resource_demands (uid)");

        ddlogAPI = compileAndLoad(
                ddl.stream().map(CalciteSqlStatement::new).collect(Collectors.toList()),
                new CalciteToPrestoTranslator(),
                indexStatements);

        ToH2Translator<CalciteSqlStatement> translator = new CalciteToH2Translator();
        // Initialise the data provider
        provider = new DDlogJooqProvider(ddlogAPI,
                Stream.concat(ddl.stream().map(x -> translator.toH2(new CalciteSqlStatement(x))),
                                indexStatements.stream().map(H2SqlStatement::new))
                        .collect(Collectors.toList()));
        MockConnection connection = new MockConnection(provider);

        // Pass the mock connection to a jOOQ DSLContext
        create = DSL.using(connection);
    }

    private Insert updateNodeRecord(final Node node, final DSLContext conn) {
        final NodeStatus status = node.getStatus();
        boolean outOfDisk = false;
        boolean memoryPressure = false;
        boolean diskPressure = false;
        boolean pidPressure = false;
        boolean networkUnavailable = false;
        boolean ready = true;
        final boolean getUnschedulable = node.getSpec().getUnschedulable() != null
                && node.getSpec().getUnschedulable();
        for (final NodeCondition condition : status.getConditions()) {
            final boolean value = condition.getStatus().equals("True");
            switch (condition.getType()) {
                case "OutOfDisk" : {
                    outOfDisk = value;
                    break;
                }
                case "MemoryPressure" : {
                    memoryPressure = value;
                    break;
                }
                case "DiskPressure" : {
                    diskPressure = value;
                    break;
                }
                case "PIDPressure" : {
                    pidPressure = value;
                    break;
                }
                case "NetworkUnavailable" : {
                    networkUnavailable = value;
                    break;
                }
                case "Ready" : {
                    ready = value;
                    break;
                }
                default : {
                    throw new IllegalStateException("Unknown condition type " + condition.getType());
                }
            }
        }
        return conn.insertInto(DSL.table("NODE_INFO"),
                        DSL.field("UID"),
                        DSL.field("NAME"),
                        DSL.field("UNSCHEDULABLE"),
                        DSL.field("OUT_OF_DISK"),
                        DSL.field("MEMORY_PRESSURE"),
                        DSL.field("DISK_PRESSURE"),
                        DSL.field("PID_PRESSURE"),
                        DSL.field("READY"),
                        DSL.field("NETWORK_UNAVAILABLE"))
                .values(node.getMetadata().getUid(),
                        node.getMetadata().getName(),
                        getUnschedulable,
                        outOfDisk,
                        memoryPressure,
                        diskPressure,
                        pidPressure,
                        ready,
                        networkUnavailable
                );
    }

    private List<Insert> addNodeLabels(final DSLContext conn, final Node node) {
        final Map<String, String> labels = node.getMetadata().getLabels();
        return labels.entrySet().stream().map(
                (label) -> conn.insertInto(DSL.table("NODE_LABELS"))
                        .values(node.getMetadata().getName(), label.getKey(), label.getValue())
        ).collect(Collectors.toList());
    }

    private List<Insert> addNodeTaints(final DSLContext conn, final Node node) {
        if (node.getSpec().getTaints() == null) {
            return Collections.emptyList();
        }
        return node.getSpec().getTaints().stream().map(taint ->
                conn.insertInto(DSL.table("NODE_TAINTS"))
                        .values(node.getMetadata().getName(),
                                taint.getKey(),
                                taint.getValue() == null ? "" : taint.getValue(),
                                taint.getEffect())
        ).collect(Collectors.toList());
    }

    private List<Insert> addNodeImages(final DSLContext conn, final Node node) {
        final List<Insert> inserts = new ArrayList<>();
        for (final ContainerImage image: node.getStatus().getImages()) {
            for (final String imageName: Optional.ofNullable(image.getNames()).orElse(Collections.emptyList())) {
                final int imageSizeInMb = (int) (((float) image.getSizeBytes()) / 1024 / 1024);
                inserts.add(
                        conn.insertInto(DSL.table("NODE_IMAGES"))
                                .values(node.getMetadata().getName(), imageName, imageSizeInMb)
                );
            }
        }
        return inserts;
    }

    private List<Insert> addNodeCapacities(final DSLContext conn, final Node node) {
        final Map<String, Quantity> allocatable = node.getStatus().getAllocatable();
        return allocatable.entrySet().stream().map(
                (es) -> conn.insertInto(DSL.table("NODE_RESOURCES"))
                        .values(node.getMetadata().getUid(), es.getKey(), convertUnit(es.getValue(), es.getKey()))
        ).collect(Collectors.toList());
    }

    static long convertUnit(final Quantity quantity, final String resourceName) {
        if (resourceName.equals("cpu")) {
            final String unit = quantity.getFormat();
            if (unit.equals("")) {
                // Convert to milli-cpu
                return (long) (Double.parseDouble(quantity.getAmount()) * 1000);
            } else if (unit.equals("m")) {
                return (long) (Double.parseDouble(quantity.getAmount()));
            }
            throw new IllegalArgumentException(quantity + " for resource type: " + resourceName);
        } else {
            // Values are guaranteed to be under 2^63 - 1, so this is safe
            return Quantity.getAmountInBytes(quantity).longValue();
        }
    }

    private void onAddSync(final Node node) {
        final long now = System.nanoTime();

        final List<Query> queries = new ArrayList<>();
        queries.add(updateNodeRecord(node, create));
        queries.addAll(addNodeLabels(create, node));
        queries.addAll(addNodeTaints(create, node));
        queries.addAll(addNodeImages(create, node));
        queries.addAll(addNodeCapacities(create, node));
        create.batch(queries).execute();

        System.out.printf("%s node added in %d ns\n", node.getMetadata().getName(), (System.nanoTime() - now));
    }

    private static Node addNode(final String nodeName, final UUID uid, final Map<String, String> labels,
                                final List<NodeCondition> conditions) {
        final Node node = new Node();
        final NodeStatus status = new NodeStatus();
        final Map<String, Quantity> quantityMap = new HashMap<>();
        quantityMap.put("cpu", new Quantity("10000"));
        quantityMap.put("memory", new Quantity("10000"));
        quantityMap.put("ephemeral-storage", new Quantity("10000"));
        quantityMap.put("pods", new Quantity("100"));
        status.setCapacity(quantityMap);
        status.setAllocatable(quantityMap);
        status.setImages(Collections.emptyList());
        node.setStatus(status);
        status.setConditions(conditions);
        final NodeSpec spec = new NodeSpec();
        spec.setUnschedulable(false);
        spec.setTaints(Collections.emptyList());
        node.setSpec(spec);
        final ObjectMeta meta = new ObjectMeta();
        meta.setUid(uid.toString());
        meta.setName(nodeName);
        meta.setLabels(labels);
        node.setMetadata(meta);
        return node;
    }

    @Test
    public void slowJoinTest() {
        int numNodes = 50000;

        for (int i = 0; i < numNodes; i++) {
            final String nodeName = "n" + i;
            final Node node = addNode(nodeName, UUID.randomUUID(), Collections.emptyMap(), Collections.emptyList());
            node.getStatus().getCapacity().put("cpu", new Quantity("2"));
            node.getStatus().getCapacity().put("memory", new Quantity("2000"));
            node.getStatus().getCapacity().put("pods", new Quantity("110"));
            onAddSync(node);

            // Add one system pod per node
           /* final String podName = "system-pod-" + nodeName;
            final String status = "Running";
            final Pod pod = newPod(podName, UUID.randomUUID(), status, Collections.emptyMap(), Collections.emptyMap());
            final Map<String, Quantity> resourceRequests = new HashMap<>();
            resourceRequests.put("cpu", new Quantity("100m"));
            resourceRequests.put("memory", new Quantity("1"));
            resourceRequests.put("pods", new Quantity("1"));
            pod.getMetadata().setNamespace("kube-system");
            pod.getMetadata().setResourceVersion("1");
            pod.getSpec().getContainers().get(0).getResources().setRequests(resourceRequests);
            pod.getSpec().setNodeName(nodeName);
            handler.onAddSync(pod);*/
        }
    }


}
