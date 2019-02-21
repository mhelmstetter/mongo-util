package com.mongodb.util;

import static java.util.Arrays.asList;
import static java.util.Map.Entry.comparingByKey;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.Document;
import org.bson.types.BasicBSONList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShapeUtil {

    protected static final Logger logger = LoggerFactory.getLogger(ShapeUtil.class);

    private String dbName;

    private static final Set<String> EXPRESSION_OPERATORS = asSet(Identifier.AND, Identifier.OR, Identifier.NOR);

    private static final Set<String> ALL_OPERATORS = asSet(Identifier.AND, Identifier.OR, Identifier.NOR, "$in", "$gt",
            "$lt", "$eq", "$ne", "$nin", "$exists", "$gte", "find", "sort", "distinct", "aggregate");

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> Set<T> asSet(final T... elements) {
        return new HashSet<>(asList(elements));
    }

    private static boolean isExpressionOperator(final String pKey) {
        return EXPRESSION_OPERATORS.contains(pKey.trim().toLowerCase());
    }

    private static boolean isOperator(final String pKey) {
        return pKey.startsWith("$") || pKey.equals("find");
    }
    
    public static Set<String> getShape(final Document predicateDoc) {

        if (predicateDoc == null) {
            System.out.println();
        }
        TreeSet<String> predicates = new TreeSet<String>();

        predicateDocToShapeRecursive(predicateDoc, predicates);

        return predicates;
    }

    public static Document predicateDocToShapeRecursive(final Document pPredicateDoc,
            Set<String> predicates) {
        final Document shapePredicate = new Document();
        for (final Map.Entry<String, Object> entry : pPredicateDoc.entrySet()) {
            final String key = entry.getKey();
            final Object valueObj = entry.getValue();

            final Object shapeValue;
            if (isExpressionOperator(key)) { // eg. "$and", "$or", "$nor"
                if (valueObj instanceof List) { // list of expressions
                    shapeValue = expressionListToShapeValueRecursive(key, (List) valueObj, predicates);
                } else {
                    logger.warn("Unexpected value type {} for key {}; collapsing to 1",
                            valueObj.getClass().getSimpleName(), key);
                    shapeValue = 1;
                }
            } else if (Identifier.isDocWithNestedOperators(valueObj)) {
                shapeValue = predicateDocToShapeRecursive((Document) valueObj, predicates);
            } else {
                shapeValue = 1;
            }

            if (!isOperator(key)) {
                predicates.add(key);
            }

            shapePredicate.append(key, shapeValue);
        }

        return sortPredicateDoc(shapePredicate);
    }

    private static Document sortPredicateDoc(final Document pPredicateDoc) {
        return pPredicateDoc.entrySet().stream().sorted(comparingByKey()).collect(toBasicBSONObject());
    }

    private static List<Document> expressionListToShapeValueRecursive(final String key,
            final List<Document> pExpressionList, Set<String> predicates) {

        final List<Document> shapeList = new ArrayList<Document>();

        for (final Object elemObj : pExpressionList) {
            if (elemObj instanceof Document) {
                final Document doc = (Document) elemObj;
                final Document shapeDoc = predicateDocToShapeRecursive(doc, predicates);
                shapeList.add(shapeDoc);
            } else {
                logger.warn("Unexpected expression element type {} in expression list for key {}; ignoring",
                        elemObj.getClass().getSimpleName(), key);
            }
        }

        // we DON'T sort the list, because expressions may be short-circuited,
        // so differently-ordered
        // expressions may have very different performance, and rely on
        // different indexes

        return shapeList;
    }

    public static Collector<Map.Entry<String, Object>, Document, Document> toBasicBSONObject() {
        return new BasicBSONObjectCollector();
    }

    public static class BasicBSONObjectCollector
            implements Collector<Map.Entry<String, Object>, Document, Document> {

        @Override
        public Supplier<Document> supplier() {
            return Document::new;
        }

        @Override
        public BiConsumer<Document, Map.Entry<String, Object>> accumulator() {
            return (bsonObj, entry) -> bsonObj.append(entry.getKey(), entry.getValue());
        }

        @Override
        public BinaryOperator<Document> combiner() {
            return (left, right) -> {
                left.putAll((Document) right);
                return left;
            };
        }

        @Override
        public Function<Document, Document> finisher() {
            return Function.identity();
        }

        @Override
        public Set<Characteristics> characteristics() {
            return EnumSet.of(Characteristics.IDENTITY_FINISH);
        }
    }

}
