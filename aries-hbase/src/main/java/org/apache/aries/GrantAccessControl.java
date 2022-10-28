/*
 * Copyright (c) 2019 R.C
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.aries;

import org.apache.aries.common.RETURN_CODE;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.aries.common.EnumParameter;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.StringArrayParameter;
import org.apache.aries.common.ToyUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

@SuppressWarnings("rawtypes")
public class GrantAccessControl extends AbstractHBaseToy {

  protected final Parameter<Enum> g_v =
      EnumParameter.newBuilder("gac.grant_revoke", G_V.G, G_V.class).setRequired()
                   .setDescription("Grant or revoke permission, set either G or V")
                   .opt();
  private final Parameter<Enum> scope =
      EnumParameter.newBuilder("gac.scope", SCOPE.TABLE, SCOPE.class).setRequired()
                   .setDescription("Permission scope, set either NAMESPACE or TABLE")
                   .opt();
  protected final Parameter<Enum> relation =
      EnumParameter.newBuilder("gac.relation", RELATION.MULTI2ONE, SCOPE.class).setDescription("pending").setRequired().opt();
  protected final Parameter<String[]> tables =
      StringArrayParameter.newBuilder("gac.target_tables")
                          .setDescription("Target tables or patterns which should be started with #, delimited by ','. It must be set when gac.scope is set TABLE")
                          .opt();
  protected final Parameter<String[]> namespaces =
      StringArrayParameter.newBuilder("gac.target_namespaces")
                          .setDescription("Namespaces to be granted. It must be set when gac.scope is set NAMESPACE")
                          .opt();
  protected final Parameter<String[]> users =
      StringArrayParameter.newBuilder("gac.users").setDescription("Users to be granted ACL").setRequired().opt();
  protected final Parameter<String[]> permissions =
      StringArrayParameter.newBuilder("gac.permissions")
                          .setDescription("ACL permissions, options are R,W,X,C,A, respectively represents Read, Write, Execute, Admin, Create").opt();

  enum G_V {
    G, V
  }

  enum SCOPE {
    NAMESPACE, TABLE
  }

  enum RELATION {
    ONE2MULTI,  /** one user, multi permissions to multi tables **/
    MULTI2ONE,  /** multi users to multi tables, with the same permission **/
    MULTI2MULTI /** multi users to multi tables, with multi persmissions, but permission is bound to table **/
  }

  private G_V action;
  private RELATION map_relation;
  private Admin admin;
  private SCOPE gv_scope;

  @Override protected void requisite(List<Parameter> requisites) {
    requisites.add(g_v);
    requisites.add(scope);
    requisites.add(relation);
    requisites.add(tables);
    requisites.add(namespaces);
    requisites.add(users);
    requisites.add(permissions);
  }

  @Override
  protected void exampleConfiguration() {
    example(g_v.key(), "G");
    example(scope.key(), "TABLE");
    example(relation.key(), "MULTI2ONE");
    example(tables.key(), "what:foo,hello:world,#test:.*");
    example(users.key(), "alice,bob,foo");
    example(permissions.key(), "RW");
  }

  @Override protected void midCheck() {
    map_relation    = (RELATION) relation.value();
    action          = (G_V) g_v.value();
    gv_scope        = (SCOPE) scope.value();
    switch (map_relation) {
      case ONE2MULTI:
            ToyUtils.assertLengthValid(users.value(), 1);
      case MULTI2MULTI: {
        if (action == G_V.G && gv_scope == SCOPE.TABLE)
            ToyUtils.assertLengthValid(tables.value(), permissions.value().length);
        if (action == G_V.G && gv_scope == SCOPE.NAMESPACE)
            ToyUtils.assertLengthValid(namespaces.value(), permissions.value().length);
      }        break;
      case MULTI2ONE: {
        ToyUtils.assertLengthValid(permissions.value(), 1);
      }        break;
             default:
               break;
    }
  }

  @Override protected void buildToy(ToyConfiguration configuration) throws Exception {
    super.buildToy(configuration);
    admin = connection.getAdmin();
  }

  @Override protected int haveFun() throws Exception {
    if (!AccessControlClient.isAccessControllerRunning(connection)) {
      LOG.warning("ACL is disabled");
      return RETURN_CODE.FAILURE.code();
    }

    switch (map_relation) {
      case MULTI2MULTI: {
        switch (gv_scope) {
          case TABLE: {
            List<TableName> targetTables = grepTables();
            LOG.info("Target tables are " +  targetTables.toString());
            IntStream.range(0, users.value().length).forEach(i ->
                IntStream.range(0, targetTables.size()).forEach(j ->
                    performTablePermission(action, targetTables.get(j), users.value()[i], extractPermissionActions(action == G_V.G ? permissions.value()[j] : "RWXCA"))
                )
            );
          } break;
          case NAMESPACE: {
            LOG.info("Target namespaces are " + namespaces.valueInString());
            IntStream.range(0, users.value().length).forEach(i ->
                IntStream.range(0, namespaces.value().length).forEach(j ->
                    performNamespacePermissions(action, namespaces.value()[j], users.value()[i], extractPermissionActions(action == G_V.G ? permissions.value()[j] : "RWXCA"))
                )
            );
          } break;
          default:
            break;
        }
      } break;
      case ONE2MULTI: {
        String user = users.value()[0];
        switch (gv_scope) {
          case TABLE: {
            List<TableName> targetTables = grepTables();
            LOG.info("Target tables are " +  targetTables.toString());
            IntStream.range(0, targetTables.size())
                     .forEach(i -> performTablePermission(action, targetTables.get(i), user, extractPermissionActions(action == G_V.G ? permissions.value()[i] : "RWXCA")));
          } break;
          case NAMESPACE: {
            LOG.info("Target namespaces are " + namespaces.valueInString());
            IntStream.range(0, namespaces.value().length)
                     .forEach(i -> performNamespacePermissions(action, namespaces.value()[i], user, extractPermissionActions(action == G_V.G ? permissions.value()[i] : "RWXCA")));
          } break;
        }
      } break;
      case MULTI2ONE: {
        String[] users_name = users.value();
        switch (gv_scope) {
          case TABLE: {
            List<TableName> targetTables = grepTables();
            LOG.info("Target tables are " +  targetTables.toString());
            IntStream.range(0, users_name.length).forEach(i ->
                IntStream.range(0, targetTables.size()).forEach(j ->
                    performTablePermission(action, targetTables.get(j), users_name[i], extractPermissionActions(action == G_V.G ? permissions.value()[0] : "RWXCA"))
                )
            );
          } break;
          case NAMESPACE: {
            LOG.info("Target namespaces are " + namespaces.valueInString());
            IntStream.range(0, users_name.length).forEach(i ->
                IntStream.range(0, namespaces.value().length).forEach(j ->
                    performNamespacePermissions(action, namespaces.value()[j], users_name[i], extractPermissionActions(action == G_V.G ? permissions.value()[0] : "RWXCA"))
                )
            );
          } break;
          default:
            break;
        }
      }
      default:
        break;
    }

    return RETURN_CODE.SUCCESS.code();
  }

  protected Permission.Action[] extractPermissionActions(String permission_actions) {
    Set<Permission.Action> actions = new HashSet<>();
    for (char c : permission_actions.toCharArray()) {
      switch (c) {
        case 'R': actions.add(Permission.Action.READ);   break;
        case 'W': actions.add(Permission.Action.WRITE);  break;
        case 'C': actions.add(Permission.Action.CREATE); break;
        case 'X': actions.add(Permission.Action.EXEC);   break;
        case 'A': actions.add(Permission.Action.ADMIN);  break;
         default:                                        break;
      }
    }
    return actions.toArray(new Permission.Action[0]);
  }

  private List<TableName> grepTables() throws IOException  {
    List<TableName> tablenames = new ArrayList<>();
    for (String table_or_pattern : tables.value()) {
      if (table_or_pattern.startsWith("#")) {
        tablenames.addAll(Arrays.asList(admin.listTableNames(Pattern.compile(table_or_pattern.substring(1)))));
      } else {
        tablenames.add(TableName.valueOf(table_or_pattern));
      }
    }
    return tablenames;
  }

  private void performNamespacePermissions(G_V act, String namespace, String user_name, Permission.Action[] actions) {
    try {
      switch (act) {
        case G: {
          logging("Granting", user_name, "namespace", namespace, actions);
          AccessControlClient.grant(connection, namespace, user_name, actions);
          break;
        }
        case V: {
          logging("Revoking", user_name, "namespace", namespace, actions);
          AccessControlClient.revoke(connection, namespace, user_name, actions);
          break;
        }
       default:
         break;
      }
    } catch (Throwable t) {
      LOG.warning("Can't " + act + " " + user_name + " permissions on " + namespace);
    }
  }

  protected void performTablePermission(G_V act, TableName table, String user_name, Permission.Action[] actions) {
    try {
      switch (act) {
        case G: {
          logging("Granting", user_name, "table", table.getNameAsString(), actions);
          AccessControlClient.grant(connection, table, user_name, null, null, actions);
          break;
        }
        case V: {
          logging("Revoking", user_name, "table", table.getNameAsString(), actions);
          AccessControlClient.revoke(connection, table, user_name, null, null, actions);
          break;
        }
       default:
         break;
      }
    } catch (Throwable t) {
      LOG.warning("Can't " + act + " " + user_name + " permissions on " + table);
    }
  }

  @Override protected void destroyToy() throws Exception {
    super.destroyToy();
    admin.close();
  }

  private void logging(String g_v, String user, String scope, String scope_name, Permission.Action[] actions) {
    LOG.info(g_v + " " + user + " permissions: " + ToyUtils.arrayToString(actions) + " on " + scope + ": " + scope_name );
  }

  @Override protected String getParameterPrefix() {
    return "gac";
  }

}
