package org.apache.aries;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.swagger.client.ApiException;
import io.swagger.client.api.SeekerControllerApi;
import io.swagger.client.model.EndPointDescriptor;
import io.swagger.client.model.TicketDescriptor;
import io.swagger.client.model.TicketResponseStatus;
import io.swagger.client.model.TicketStatusDescriptor;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.StringParameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;

public class MetaCollector extends AbstractHBaseToy {
  private final Parameter<String> admin_names =
      StringParameter.newBuilder("mc.admin_names").setRequired()
          .setDescription("The names of the cluster administrator.").opt();
  private final Parameter<String> location =
      StringParameter.newBuilder("mc.location")
          .setDescription("The abbreviation of the cluster location.")
          .setRequired().opt();
  private final Parameter<String> time_zone =
      StringParameter.newBuilder("mc.timezone")
          .setDescription("The timezone of the cluster.")
          .setRequired().opt();
  private final Parameter<String> cluster_type =
      StringParameter.newBuilder("mc.cluster_type").setRequired()
          .setDescription("The cluster type prod or test.")
          .setRequired().opt();
  private static final String ADMIN_NAMES = "hbase.admin.names";
  private static final String CLUSTER_LOCATION = "hbase.cluster.location";
  private static final String CLUSTER_TIMEZONE = "hbase.cluster.timezone";
  private static final String CLUSTER_TIMEZONE_DEFAULT = "GMT+8";
  private static final String CLUSTER_TYPE = "hbase.cluster.type";
  private static final String CLUSTER_TYPE_DEFAULT = "prod";

  private Admin admin;
  private MetaExtractor extractor;

  @Override
  protected String getParameterPrefix() {
    return "mc";
  }

  @Override
  protected void requisite(List<Parameter> requisites) {
    requisites.add(admin_names);
    requisites.add(location);
    requisites.add(time_zone);
    requisites.add(cluster_type);
  }

  @Override
  protected void exampleConfiguration() {
    example(admin_names.key(), "san.zhang");
    example(location.key(), "SG");
    example(time_zone.key(), "GMT+8");
    example(cluster_type.key(), "test");
  }

  @Override
  protected void buildToy(ToyConfiguration configuration) throws Exception {
    super.buildToy(configuration);
    admin = connection.getAdmin();
    Configuration conf = ConfigurationFactory.createHBaseConfiguration(configuration);
    conf.set(ADMIN_NAMES, admin_names.value());
    conf.set(CLUSTER_LOCATION, location.value());
    conf.set(CLUSTER_TIMEZONE, time_zone.value());
    conf.set(CLUSTER_TYPE, cluster_type.value());
    extractor = new MetaExtractor(conf);
  }

  @Override
  protected int haveFun() throws Exception {
    HTableDescriptor[] tables = admin.listTables();
    for (HTableDescriptor t : tables) {
      doOperation(t, true, "@updateMetaOffline");
    }
    return 0;
  }

  @Override
  protected void destroyToy() throws Exception {
    super.destroyToy();
    admin.close();
  }

  private void doOperation(Object metaEntity, boolean isActive, String operationType) {
      if (metaEntity instanceof HTableDescriptor) {
        MetaSender.sendJson(extractor.metaFromTableDesc(
            (HTableDescriptor) metaEntity, isActive), operationType);
      } else if (metaEntity instanceof TableName) {
        MetaSender.sendJson(extractor.metaFromTableName(
            (TableName) metaEntity, isActive), operationType);
      }
  }


  static class MetaExtractor {

    static Gson gson = new Gson();

    private final Configuration conf;

    public MetaExtractor(Configuration conf) {
      this.conf = conf;
    }

    public JsonObject metaFromTableDesc(HTableDescriptor desc, boolean isActive) {
      JsonObject jsonObject = metaFromTableName(desc.getTableName(), isActive);

      List<Map<String, String>> columnFamilies = new ArrayList<>(desc.getFamilies().size());
      for (HColumnDescriptor column : desc.getColumnFamilies()) {
        columnFamilies.add(extractColumnMeta(column));
      }
      String project = desc.getValue("PROJECT");
      String team = desc.getValue("TEAM");
      String owners = desc.getValue("TABLE_OWNERS");
      jsonObject.addProperty("project", project == null ? "" : project);
      jsonObject.addProperty("team", team == null ? "" : team);
      jsonObject.addProperty("businessPic", owners);
      jsonObject.addProperty("techPic", owners);

      JsonObject attributes = (JsonObject) jsonObject.get("attributes");
      attributes.add("columnFamilies", gson.toJsonTree(columnFamilies));
      attributes.addProperty("owners", owners);
      attributes.addProperty("tableName", desc.getNameAsString());
      attributes.addProperty("durability", desc.getDurability().toString());

      jsonObject.add("attributes", attributes);
      return jsonObject;
    }

    public JsonObject metaFromTableName(TableName tableName, boolean isActive) {
      JsonObject jsonObject = new JsonObject();
      String region = conf.get(CLUSTER_LOCATION, "");
      String adminNames = conf.get(ADMIN_NAMES, "");
      long now = System.currentTimeMillis();

      jsonObject.addProperty("qualifiedName", "hbase@" + region + "@" + tableName.getNameAsString());
      jsonObject.addProperty("serviceType", "hbase");
      jsonObject.addProperty("typeName", "hbaseTable");
      jsonObject.addProperty("name", "");
      jsonObject.addProperty("status", isActive ? "active" : "delete");
      jsonObject.addProperty("region", region);
      jsonObject.addProperty("project", "");
      jsonObject.addProperty("team", "");
      jsonObject.addProperty("env", conf.get(CLUSTER_TYPE, CLUSTER_TYPE_DEFAULT));
      jsonObject.addProperty("description", "");
      jsonObject.addProperty("businessPic", "");
      jsonObject.addProperty("techPic", "");
      jsonObject.addProperty("customAttributes", "");
      jsonObject.addProperty("businessCreateTime", now);
      jsonObject.addProperty("businessUpdateTime", now);
      jsonObject.addProperty("createBy", adminNames);
      jsonObject.addProperty("updateBy", adminNames);
      jsonObject.addProperty("timezone", conf.get(CLUSTER_TIMEZONE, CLUSTER_TIMEZONE_DEFAULT));
      jsonObject.add("attributes", new JsonObject());

      return jsonObject;
    }

    public static Map<String, String> extractColumnMeta(HColumnDescriptor desc) {
      Map<String, String> map = new HashMap<>();
      map.put("name", desc.getNameAsString());
      map.put("bloomFilterType", desc.getBloomFilterType().name());
      map.put("compressionType", desc.getCompressionType().getName());
      map.put("maxVersion", Integer.toString(desc.getMaxVersions()));
      map.put("minVersion", Integer.toString(desc.getMinVersions()));
      map.put("ttl", Long.toString(desc.getTimeToLive()));
      return map;
    }
  }

  static class MetaSender {
    private static final Logger logger = Logger.getLogger(MetaSender.class.getName());
    private static final SeekerControllerApi api = new SeekerControllerApi();
    private static final String TOPIC = "di.datamap_hbase_metadata_collect";

    public static void sendJson(JsonObject jsonObject, String operationType) {
      String correlationId = UUID.randomUUID().toString();
      Map<String, String> data = new Gson().fromJson(jsonObject.toString(), Map.class);
      EndPointDescriptor endpoint = new EndPointDescriptor().api("HBASE").data(data)
          .service("urn:HBASE:PUSH_MESSAGE:" + TOPIC);
      TicketStatusDescriptor ticketStatusDescriptor =
          new TicketStatusDescriptor().status("POST").ticketId(correlationId).data(
              Collections.singletonMap("some", "data"));
      TicketDescriptor body = new TicketDescriptor().correlationId(correlationId)
          .description("Running from bifrost-seeker-client").domainURN("urn:HBASE:SEJBU0U=")
          .endpoint(endpoint).id(null).name("some-name").namespace("HBASE")
          .status(ticketStatusDescriptor)
          .ticketName(jsonObject.get("qualifiedName").toString() + operationType)
          .event("PUSH_MESSAGE_EVENT");
      TicketResponseStatus ticketResponseStatus;
      try {
        ticketResponseStatus = api.createTicketUsingPOST(body, null, null);
        logger.info("EDA message sending successes. " + ticketResponseStatus.toString());
      } catch (ApiException e) {
        logger.warning(
            "EDA message failed. Target info: " + jsonObject.toString()
                + "Error message: " + e.getMessage());
      }
    }
  }
}
