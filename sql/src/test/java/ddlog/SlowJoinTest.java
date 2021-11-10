package ddlog;

import com.vmware.ddlog.DDlogJooqProvider;
import com.vmware.ddlog.ir.DDlogProgram;
import com.vmware.ddlog.translator.Translator;
import com.vmware.ddlog.util.sql.*;
import ddlogapi.DDlogAPI;
import ddlogapi.DDlogException;
import io.fabric8.kubernetes.api.model.*;
import org.jooq.DSLContext;
import org.jooq.Insert;
import org.jooq.InsertOnDuplicateStep;
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
    private static boolean manualOptimization = false;
    protected static DDlogAPI ddlogAPI;
    protected static DSLContext create;
    protected static DDlogJooqProvider provider;

    public static <R extends SqlStatement> DDlogAPI compileAndLoad(
            final List<R> ddl, final ToPrestoTranslator<R> translator,
            final List<String> createIndexStatements) throws IOException, DDlogException {
        final String fileName = "/tmp/program.dl";
        if (!manualOptimization) {
            final Translator t = new Translator(null);
            ddl.forEach(x -> t.translateSqlStatement(translator.toPresto(x)));
            createIndexStatements.forEach(t::translateCreateIndexStatement);

            final DDlogProgram dDlogProgram = t.getDDlogProgram();
            File tmp = new File(fileName);
            BufferedWriter bw = new BufferedWriter(new FileWriter(tmp));
            bw.write(dDlogProgram.toString());
            bw.close();
        }
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
        ddl.add("create table pod_images\n" +
                "(\n" +
                "  pod_uid varchar(36) not null,\n" +
                "  image_name varchar(200) not null,\n" +
                "  primary key (pod_uid)\n" +
                ")");
        ddl.add("create table pod_ports_request\n" +
                "(\n" +
                "  pod_uid varchar(36) not null,\n" +
                "  host_ip varchar(100) not null,\n" +
                "  host_port integer not null,\n" +
                "  host_protocol varchar(10) not null,\n" +
                "  primary key (pod_uid)\n" +
                ")");
        ddl.add("create table pod_tolerations\n" +
                "(\n" +
                "  pod_uid varchar(36) not null,\n" +
                "  tolerations_key varchar(317),\n" +
                "  tolerations_value varchar(63),\n" +
                "  tolerations_effect varchar(100),\n" +
                "  tolerations_operator varchar(100),\n" +
                "  primary key(pod_uid)\n" +
                ")");

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
        indexStatements.add("create index pod_ports_request_by_uid on pod_ports_request (pod_uid)");

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

    private boolean hasNodeSelector(final Pod pod) {
        final PodSpec podSpec = pod.getSpec();
        return  (podSpec.getNodeSelector() != null && podSpec.getNodeSelector().size() > 0)
                || (podSpec.getAffinity() != null
                && podSpec.getAffinity().getNodeAffinity() != null
                && podSpec.getAffinity().getNodeAffinity()
                .getRequiredDuringSchedulingIgnoredDuringExecution() != null
                && podSpec.getAffinity().getNodeAffinity()
                .getRequiredDuringSchedulingIgnoredDuringExecution()
                .getNodeSelectorTerms().size() > 0);
    }

    private long equivalenceClassHash(final Pod pod) {
        return Objects.hash(pod.getMetadata().getNamespace(),
                pod.getMetadata().getLabels(),
                pod.getSpec().getAffinity(),
                pod.getSpec().getInitContainers(),
                pod.getSpec().getNodeName(),
                pod.getSpec().getNodeSelector(),
                pod.getSpec().getTolerations(),
                pod.getSpec().getVolumes());
    }

    enum QosClass {
        Guaranteed,
        BestEffort,
        Burstable
    }

    private QosClass getQosClass(final List<ResourceRequirements> resourceRequirements) {
        final List<String> supportedResources = new ArrayList<>();
        supportedResources.add("cpu");
        supportedResources.add("memory");
        boolean isGuaranteed = true;
        boolean bestEffort = true;
        for (final ResourceRequirements reqs: resourceRequirements) {
            for (final String supportedResource: supportedResources) {
                final Quantity request = reqs.getRequests() == null ? null : reqs.getRequests().get(supportedResource);
                final Quantity limit = reqs.getLimits() == null ? null : reqs.getLimits().get(supportedResource);
                if (request != null || limit != null) {
                    bestEffort = false;
                }
                if (request == null || !request.equals(limit)) {
                    isGuaranteed = false;
                }
            }
        }

        if (bestEffort) {
            return QosClass.BestEffort;
        }
        if (isGuaranteed) {
            return QosClass.Guaranteed;
        }
        return QosClass.Burstable;
    }

    private List<Query> updatePodRecord(final Pod pod, final DSLContext conn) {
        final List<Query> inserts = new ArrayList<>();
        final List<ResourceRequirements> resourceRequirements = pod.getSpec().getContainers().stream()
                .map(Container::getResources)
                .collect(Collectors.toList());

        // The first owner reference is used to break symmetries.
        final List<OwnerReference> owners = pod.getMetadata().getOwnerReferences();
        final String ownerName = (owners == null || owners.size() == 0) ? "" : owners.get(0).getName();
        final boolean hasNodeSelector = hasNodeSelector(pod);

        boolean hasPodAffinityRequirements = false;
        boolean hasPodAntiAffinityRequirements = false;
        boolean hasNodePortRequirements = false;
        boolean hasPodTopologySpreadConstraints = false;

        if (pod.getSpec().getAffinity() != null && pod.getSpec().getAffinity().getPodAffinity() != null) {
            hasPodAffinityRequirements = true;
        }
        if (pod.getSpec().getAffinity() != null && pod.getSpec().getAffinity().getPodAntiAffinity() != null) {
            hasPodAntiAffinityRequirements = true;
        }
        if (pod.getSpec().getContainers() != null
                && pod.getSpec().getContainers().stream()
                .filter(c -> c.getPorts() != null).flatMap(c -> c.getPorts().stream())
                .anyMatch(containerPort -> containerPort.getHostPort() != null)) {
            hasNodePortRequirements = true;
        }
        if (pod.getSpec().getTopologySpreadConstraints() != null
                && !pod.getSpec().getTopologySpreadConstraints().isEmpty()) {
            hasPodTopologySpreadConstraints = true;
        }

        final int priority = Math.min(pod.getSpec().getPriority() == null ? 10 : pod.getSpec().getPriority(), 100);
        final long resourceVersion = Long.parseLong(pod.getMetadata().getResourceVersion());


        // In order for this insert to work for the DDlog backend, we MUST ensure the columns are ordered exactly
        // as they are ordered in the table creation SQL statement.
        final InsertOnDuplicateStep podInfoInsert = conn.insertInto(DSL.table("POD_INFO"),
                        DSL.field("UID"),
                        DSL.field("POD_NAME"),
                        DSL.field("STATUS"),
                        DSL.field("NODE_NAME"),
                        DSL.field("NAMESPACE"),
                        DSL.field("OWNER_NAME"),
                        DSL.field("CREATION_TIMESTAMP"),
                        DSL.field("PRIORITY"),
                        DSL.field("SCHEDULERNAME"),
                        DSL.field("HAS_NODE_SELECTOR_LABELS"),
                        DSL.field("HAS_POD_AFFINITY_REQUIREMENTS"),
                        DSL.field("HAS_POD_ANTI_AFFINITY_REQUIREMENTS"),
                        DSL.field("HAS_NODE_PORT_REQUIREMENTS"),
                        DSL.field("HAS_TOPOLOGY_SPREAD_CONSTRAINTS"),
                        DSL.field("EQUIVALENCE_CLASS"),
                        DSL.field("QOS_CLASS"),
                        DSL.field("RESOURCEVERSION"),
                        DSL.field("LAST_REQUEUE"))
                .values(pod.getMetadata().getUid(),
                        pod.getMetadata().getName(),
                        pod.getStatus().getPhase(),
                        pod.getSpec().getNodeName(),
                        pod.getMetadata().getNamespace(),
                        ownerName,
                        pod.getMetadata().getCreationTimestamp(),
                        priority,
                        pod.getSpec().getSchedulerName(),
                        hasNodeSelector,
                        hasPodAffinityRequirements,
                        hasPodAntiAffinityRequirements,
                        hasNodePortRequirements,
                        hasPodTopologySpreadConstraints,
                        equivalenceClassHash(pod),
                        getQosClass(resourceRequirements).toString(),
                        resourceVersion,
                        0
                );
        inserts.add(podInfoInsert);
        return inserts;
    }

    private List<Insert<?>> updateContainerInfoForPod(final Pod pod, final DSLContext conn) {
        final List<Insert<?>> inserts = new ArrayList<>();
        conn.deleteFrom(DSL.table("POD_IMAGES"))
                .where(DSL.field("POD_UID").eq(pod.getMetadata().getUid()))
                .execute();
        conn.deleteFrom(DSL.table("POD_PORTS_REQUEST"))
                .where(DSL.field("POD_UID").eq(pod.getMetadata().getUid()))
                .execute();

        // Record all unique images in the container
        pod.getSpec().getContainers().stream()
                .map(Container::getImage)
                .distinct()
                .forEach(image ->
                        inserts.add(conn.insertInto(DSL.table("POD_IMAGES")).values(pod.getMetadata().getUid(), image))
                );

        for (final Container container: pod.getSpec().getContainers()) {
            if (container.getPorts() == null || container.getPorts().isEmpty()) {
                continue;
            }
            for (final ContainerPort portInfo: container.getPorts()) {
                if (portInfo.getHostPort() == null) {
                    continue;
                }
                inserts.add(conn.insertInto(DSL.table("POD_PORTS_REQUEST"))
                        .values(pod.getMetadata().getUid(),
                                portInfo.getHostIP() == null ? "0.0.0.0" : portInfo.getHostIP(),
                                portInfo.getHostPort(),
                                portInfo.getProtocol()));
            }
        }
        return inserts;
    }

    private List<Insert> updatePodLabels(final Pod pod, final DSLContext conn) {
        // Update pod_labels table. This will be used for managing affinities, I think?
        final Map<String, String> labels = pod.getMetadata().getLabels();
        if (labels != null) {
            return labels.entrySet().stream().map(
                    (label) -> conn.insertInto(DSL.table("POD_LABELS"))
                            .values(pod.getMetadata().getUid(), label.getKey(), label.getValue())
            ).collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    private List<Insert> updatePodTolerations(final Pod pod, final DSLContext conn) {
        if (pod.getSpec().getTolerations() == null) {
            return Collections.emptyList();
        }
        conn.deleteFrom(DSL.table("POD_TOLERATIONS"))
                .where(DSL.field("POD_UID").eq(pod.getMetadata().getUid())).execute();
        final List<Insert> inserts = new ArrayList<>();
        for (final Toleration toleration: pod.getSpec().getTolerations()) {
            inserts.add(conn.insertInto(DSL.table("POD_TOLERATIONS"))
                    .values(pod.getMetadata().getUid(),
                            toleration.getKey() == null ? "" : toleration.getKey(),
                            toleration.getValue() == null ? "" : toleration.getValue(),
                            toleration.getEffect() == null ? "" : toleration.getEffect(),
                            toleration.getOperator() == null ? "Equal" : toleration.getOperator()));
        }
        return Collections.unmodifiableList(inserts);
    }

    private List<Insert<?>> updateResourceRequests(final Pod pod, final DSLContext conn) {
        conn.deleteFrom(DSL.table("POD_RESOURCE_DEMANDS"))
                .where(DSL.field("UID").eq(pod.getMetadata().getUid())).execute();
        final List<Insert<?>> inserts = new ArrayList<>();
        final Map<String, Long> resourceRequirements = pod.getSpec().getContainers().stream()
                .map(Container::getResources)
                .filter(Objects::nonNull)
                .map(ResourceRequirements::getRequests)
                .filter(Objects::nonNull)
                .flatMap(e -> e.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey,
                        es -> convertUnit(es.getValue(), es.getKey()),
                        Long::sum));
        final Map<String, Long> overheads = pod.getSpec().getOverhead() != null ?
                pod.getSpec().getOverhead().entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey,
                                es -> convertUnit(es.getValue(), es.getKey()),
                                Long::sum)) : new HashMap<>();
        resourceRequirements.putIfAbsent("pods", 1L);
        resourceRequirements.forEach((resource, demand) -> inserts.add(
                conn.insertInto(DSL.table("POD_RESOURCE_DEMANDS"))
                        .values(pod.getMetadata().getUid(), resource, demand + overheads.getOrDefault(resource, 0L))
        ));
        return inserts;
    }

    private void addPod(final Pod pod) {
        /*if (pod.getMetadata().getUid() != null &&
                deletedUids.getIfPresent(pod.getMetadata().getUid()) != null) {
            LOG.trace("Received stale event for pod that we already deleted: {} (uid: {}, resourceVersion {}). " +
                            "Ignoring", pod.getMetadata().getName(), pod.getMetadata().getUid(),
                    pod.getMetadata().getResourceVersion());
            return;
        }*/
        final List<Query> inserts = new ArrayList<>();
        inserts.addAll(updatePodRecord(pod, create));
        inserts.addAll(updateContainerInfoForPod(pod, create));
        inserts.addAll(updatePodLabels(pod, create));
        inserts.addAll(updatePodTolerations(pod, create));
        inserts.addAll(updateResourceRequests(pod, create));
        /*inserts.addAll(updatePodAffinity(pod, conn));
        inserts.addAll(updatePodTopologySpread(pod, conn));*/
        create.batch(inserts).execute();
    }

    private static Pod newPod(final String podName, final UUID uid, final String phase,
                              final Map<String, String> selectorLabels, final Map<String, String> labels) {
        final Pod pod = new Pod();
        final ObjectMeta meta = new ObjectMeta();
        meta.setUid(uid.toString());
        meta.setName(podName);
        meta.setLabels(labels);
        meta.setCreationTimestamp("1");
        meta.setNamespace("default");
        final PodSpec spec = new PodSpec();
        spec.setSchedulerName("dcm-scheduler");
        spec.setPriority(0);
        spec.setNodeSelector(selectorLabels);

        final Container container = new Container();
        container.setName("pause");
        container.setImage("ignore");

        final ResourceRequirements resourceRequirements = new ResourceRequirements();
        resourceRequirements.setRequests(Collections.emptyMap());
        container.setResources(resourceRequirements);
        spec.getContainers().add(container);

        final Affinity affinity = new Affinity();
        final NodeAffinity nodeAffinity = new NodeAffinity();
        affinity.setNodeAffinity(nodeAffinity);
        spec.setAffinity(affinity);
        final PodStatus status = new PodStatus();
        status.setPhase(phase);
        pod.setMetadata(meta);
        pod.setSpec(spec);
        pod.setStatus(status);
        return pod;
    }

    @Test
    public void slowJoinTest() {
        int numNodes = 10000;

        for (int i = 0; i < numNodes; i++) {
            final String nodeName = "n" + i;
            final Node node = addNode(nodeName, UUID.randomUUID(), Collections.emptyMap(), Collections.emptyList());
            node.getStatus().getCapacity().put("cpu", new Quantity("2"));
            node.getStatus().getCapacity().put("memory", new Quantity("2000"));
            node.getStatus().getCapacity().put("pods", new Quantity("110"));
            onAddSync(node);

            // Add one system pod per node
            final String podName = "system-pod-" + nodeName;
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
            addPod(pod);
        }
    }


}
