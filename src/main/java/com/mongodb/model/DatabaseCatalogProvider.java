package com.mongodb.model;

import java.util.Collection;

public interface DatabaseCatalogProvider {
    DatabaseCatalog get(Collection<Namespace> namespaces);
    DatabaseCatalog get();
    
    void populateDatabaseCatalog();
}
