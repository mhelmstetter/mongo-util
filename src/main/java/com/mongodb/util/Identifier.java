package com.mongodb.util;

import org.bson.Document;

public class Identifier {

    public static final String AND = "$and";
    public static final String OR = "$or";
    public static final String NOR = "$nor";
    public static final String NOT = "$not";

    public static boolean isDocWithNestedOperators(final Object pValueObj) {
        if (pValueObj != null && pValueObj instanceof Document) {
            //final BasicBSONObject doc = (BasicBSONObject) pValueObj;
            //boolean result = doc.keySet().stream().anyMatch(Identifier::isOperator);
            return true;
        }
        return false;
    }
}