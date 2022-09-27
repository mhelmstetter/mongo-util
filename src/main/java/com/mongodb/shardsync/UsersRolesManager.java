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
	
	private final static Set<String> DATABASE_ONLY_ACTIONS = new HashSet<>(Arrays.asList("LIST_COLLECTIONS", "DB_STATS"));
	
	private final static Set<String> DATABASE_NAME_REQUIRED_ACTIONS = new HashSet<>(Arrays.asList("LIST_COLLECTIONS", "LIST_INDEXES", "COLL_STATS", "DB_STATS"));
	
	private final static Set<String> PROHIBITED_ROLES = new HashSet<>(Arrays.asList("LIST_COLLECTIONS", "DB_STATS"));

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
					
					System.out.println(role.getRole() + " '" + r.getDb() + "'");
					
					

					AtlasRoleAction a = atlasRole.getAction(convertedName);
					if (a == null) {
						a = new AtlasRoleAction();
						a.setAction(convertedName);
						
					}

					if (r.getCollection().equals("system.namespaces")) {
						logger.warn("{}: skipping {} action for {}.{}", roleName, action, r.getDb(), r.getCollection());
						continue;
					}
					
					// strip collection name for any action that Atlas doesn't allow a collection to be specified
//					if (r.getCollection().length() > 0 && DATABASE_ONLY_ACTIONS.contains(convertedName)) {
//						a.addResource(new AtlasResource(r.getDb(), ""));
//						atlasRole.addAction(a);
//					} else 
					
//					if (r.getDb().isEmpty() && DATABASE_NAME_REQUIRED_ACTIONS.contains(convertedName)) {
//						logger.warn("{}: skipping {} action", roleName, action);
//						continue;
//						
//					} else 
						if (convertedName.equals("LIST_SHARDS")) {
						AtlasResource ar = new AtlasResource();
						ar.setCluster(true);
						a.addResource(ar);
						atlasRole.addAction(a);
					} else {
						a.addResource(new AtlasResource(r.getDb(), r.getCollection()));
						atlasRole.addAction(a);
					}

					
				}
			}

			// inherited roles
			for (Role r : role.getRoles()) {
				AtlasInheritedRole inheritedRole = new AtlasInheritedRole(r.getDb(), r.getRole());
				// atlasRole.addInheritedRole(inheritedRole);
			}

			System.out.println(atlasRole);

		}
		return result;

	}

}
