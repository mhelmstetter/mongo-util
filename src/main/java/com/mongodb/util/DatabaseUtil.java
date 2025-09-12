package com.mongodb.util;

/**
 * Utility class for database-related operations and checks.
 */
public class DatabaseUtil {

    /**
     * Checks if a database name is a MongoDB system database that should typically be excluded 
     * from user operations like syncing, dropping, or analysis.
     * 
     * @param databaseName the database name to check
     * @return true if the database is a system database, false otherwise
     */
    public static boolean isSystemDatabase(String databaseName) {
        if (databaseName == null) {
            return false;
        }
        
        return databaseName.equals("admin") || 
               databaseName.equals("config") || 
               databaseName.equals("local");
    }
    
    /**
     * Checks if a database name is a MongoDB system database or contains system indicators 
     * that suggest it should be excluded from user operations.
     * This is a more comprehensive check that includes databases with "$" characters.
     * 
     * @param databaseName the database name to check
     * @return true if the database is a system database or has system indicators, false otherwise
     */
    public static boolean isSystemOrSpecialDatabase(String databaseName) {
        if (databaseName == null) {
            return false;
        }
        
        return isSystemDatabase(databaseName) ||
               databaseName.equals("system") ||
               databaseName.contains("$");
    }
}