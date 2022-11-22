package com.mongodb.shardsync;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.atlas.model.AtlasInheritedRole;
import com.mongodb.atlas.model.AtlasResource;
import com.mongodb.atlas.model.AtlasRole;
import com.mongodb.atlas.model.AtlasRoleAction;
import com.mongodb.model.Privilege;
import com.mongodb.model.Resource;
import com.mongodb.model.Role;

public class UsersRolesManager {

	private static Logger logger = LoggerFactory.getLogger(UsersRolesManager.class);

	private final static Set<String> DATABASE_ONLY_ACTIONS = new HashSet<>(
			Arrays.asList("LIST_COLLECTIONS", "DB_STATS"));

	private final static Set<String> DATABASE_NAME_REQUIRED_ACTIONS = new HashSet<>(
			Arrays.asList("LIST_COLLECTIONS", "LIST_INDEXES", "COLL_STATS", "DB_STATS"));

	private final static Set<String> SUPPORTED_ACTIONS = new HashSet<>(Arrays.asList("FIND", "INSERT", "REMOVE",
			"UPDATE", "BYPASS_DOCUMENT_VALIDATION", "USE_UUID", "CREATE_COLLECTION", "CREATE_INDEX", "DROP_COLLECTION",
			"ENABLE_PROFILER", "CHANGE_STREAM", "COLL_MOD", "COMPACT", "CONVERT_TO_CAPPED", "DROP_DATABASE",
			"DROP_INDEX", "RE_INDEX", "RENAME_COLLECTION_SAME_DB", "LIST_SESSIONS", "KILL_ANY_SESSION", "COLL_STATS",
			"CONN_POOL_STATS", "DB_HASH", "DB_STATS", "LIST_DATABASES", "LIST_COLLECTIONS", "LIST_INDEXES", "LIST_SHARDS",
			"SERVER_STATUS", "VALIDATE", "TOP", "SQL_GET_SCHEMA", "SQL_SET_SCHEMA", "VIEW_ALL_HISTORY", "OUT_TO_S3",
			"STORAGE_GET_CONFIG", "STORAGE_SET_CONFIG",
			"GET_LOG", "INDEX_STATS", "INPROG", "KILL_CURSORS", "KILLOP", "LOG_ROTATE", "REPL_SET_GET_CONFIG", "REPL_SET_GET_STATUS"));

	public static List<AtlasRole> convertMongoRolesToAtlasRoles(List<Role> roles) {

		List<AtlasRole> result = new ArrayList<AtlasRole>();
		for (Role role : roles) {

			AtlasRole atlasRole = new AtlasRole();
			String roleName = role.getRole();
			atlasRole.setRoleName(roleName);
			result.add(atlasRole);
			// target.addAction(null);

			for (Privilege p : role.getPrivileges()) {

				Resource r = p.getResource();

				for (String action : p.getActions()) {

					String convertedName = action.replaceAll("([A-Z])", "_$1").toUpperCase();
					
					if (! SUPPORTED_ACTIONS.contains(convertedName)) {
						logger.warn("{}: skipping unsupported action: {}", roleName, action);
						continue;
					}

					// System.out.println(role.getRole() + " '" + r.getDb() + "'");

					if ("listDatabases".equals(action)) {
						System.out.println();
					}

					AtlasRoleAction a = atlasRole.getAction(convertedName);
					if (a == null) {
						a = new AtlasRoleAction();
						a.setAction(convertedName);

					}

					if ("admin".equals(r.getDb())) {
						// System.out.println();
						continue;
					}

					if ("".equals(r.getDb())) {
						logger.warn("{}: skipping {} action for {}.{}", roleName, action, r.getDb(), r.getCollection());
						continue;
					}

					if ("system.indexes".equals(r.getCollection()) || "system.namespaces".equals(r.getCollection())
							|| "system.sessions".equals(r.getCollection())) {
						// logger.warn("{}: skipping {} action for {}.{}", roleName, action, r.getDb(),
						// r.getCollection());
						continue;
					}

					// strip collection name for any action that Atlas doesn't allow a collection to
					// be specified
					if (r.getCollection() != null && r.getCollection().length() > 0
							&& DATABASE_ONLY_ACTIONS.contains(convertedName)) {
						a.addResource(new AtlasResource(r.getDb(), ""));
						atlasRole.addAction(a);
					} else if ((r.getDb() == null || r.getDb().isEmpty())
							&& DATABASE_NAME_REQUIRED_ACTIONS.contains(convertedName)) {
						logger.warn("{}: skipping {} action", roleName, action);
						continue;

					} else if (convertedName.equals("LIST_SHARDS")) {
						AtlasResource ar = new AtlasResource();
						ar.setCluster(true);
						a.addResource(ar);
						atlasRole.addAction(a);
					} else {
						a.addResource(new AtlasResource(r.getDb(), r.getCollection(), r.getCluster()));
						atlasRole.addAction(a);
					}

				}
			}

			// inherited roles
			for (Role r : role.getRoles()) {
				AtlasInheritedRole inheritedRole = new AtlasInheritedRole(r.getDb(), r.getRole());
				atlasRole.addInheritedRole(inheritedRole);
			}

			System.out.println(atlasRole);

		}
		return result;

	}

}
