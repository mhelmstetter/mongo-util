package com.mongodb.diff3;

import com.mongodb.model.DatabaseCatalog;
import com.mongodb.model.Namespace;

import java.util.Collection;

public interface DatabaseCatalogProvider {
    DatabaseCatalog get(Collection<Namespace> namespaces);
    DatabaseCatalog get();
}
