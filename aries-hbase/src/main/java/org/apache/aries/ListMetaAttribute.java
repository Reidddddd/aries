package org.apache.aries;

import java.io.File;
import java.io.FileWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.aries.common.Parameter;
import org.apache.aries.common.StringParameter;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class ListMetaAttribute extends AbstractHBaseToy {
  private final Parameter<String> output_file = StringParameter.newBuilder("lma.output_file")
      .setRequired()
      .setDescription("The local property file to store the result.")
      .opt();

  private final Parameter<String> attribute = StringParameter.newBuilder("lma.attribute")
      .setRequired()
      .setDescription("The meta attribute to list.")
      .opt();

  Set<String> resultSet = new HashSet<>();

  @Override
  protected void buildToy(ToyConfiguration configuration) throws Exception {
    super.buildToy(configuration);
    File outputFile = new File(output_file.value());
    if (!outputFile.createNewFile()) {
      LOG.warning("The file " + outputFile.getName() + " already exists, will append to it.");
    }
  }

  @Override
  protected String getParameterPrefix() {
    return "lma";
  }

  @Override
  protected void requisite(List<Parameter> requisites) {
    requisites.add(output_file);
    requisites.add(attribute);
  }

  @Override
  protected void exampleConfiguration() {
    example(output_file.key(), "result.out");
    example(attribute.key(), "PROJECT, TEAM");
  }

  @Override
  protected int haveFun() throws Exception {
    HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
    HTableDescriptor[] descriptors = admin.listTables();
    for (HTableDescriptor desc : descriptors) {
      String attr = desc.getValue(attribute.value());
      if (attr != null) {
        resultSet.add(attr);
      }
    }
    FileWriter fileWriter = new FileWriter(output_file.value());
    for (String attr : resultSet) {
      fileWriter.append(attr);
      fileWriter.append('\n');
    }
    fileWriter.append(Integer.toString(resultSet.size()));
    return 0;
  }
}
