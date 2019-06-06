package liquibase.ext.hibernate.snapshot;

import liquibase.diff.compare.DatabaseObjectComparatorFactory;
import liquibase.exception.DatabaseException;
import liquibase.ext.hibernate.annotations.LiquibaseForeignKey;
import liquibase.ext.hibernate.database.HibernateDatabase;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.InvalidExampleException;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.ForeignKey;
import liquibase.structure.core.Table;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.ManyToOne;
import org.hibernate.mapping.OneToMany;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.type.Type;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;

public class ForeignKeySnapshotGenerator extends HibernateSnapshotGenerator {

    public ForeignKeySnapshotGenerator() {
        super(ForeignKey.class, new Class[]{Table.class});
    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {
        return example;
    }

    @Override
    protected void addTo(DatabaseObject foundObject, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {
        if (!snapshot.getSnapshotControl().shouldInclude(ForeignKey.class)) {
            return;
        }
        if (foundObject instanceof Table) {
            Table table = (Table) foundObject;
            HibernateDatabase database = (HibernateDatabase) snapshot.getDatabase();
            MetadataImplementor metadata = (MetadataImplementor) database.getMetadata();

            Collection<org.hibernate.mapping.Table> tmapp = metadata.collectTableMappings();
            Iterator<org.hibernate.mapping.Table> tableMappings = tmapp.iterator();
            while (tableMappings.hasNext()) {
                org.hibernate.mapping.Table hibernateTable = (org.hibernate.mapping.Table) tableMappings.next();
                Iterator fkIterator = hibernateTable.getForeignKeyIterator();
                while (fkIterator.hasNext()) {
                    org.hibernate.mapping.ForeignKey hibernateForeignKey = (org.hibernate.mapping.ForeignKey) fkIterator.next();
                    Table currentTable = new Table().setName(hibernateTable.getName());
                    currentTable.setSchema(hibernateTable.getCatalog(), hibernateTable.getSchema());

                    org.hibernate.mapping.Table hibernateReferencedTable = hibernateForeignKey.getReferencedTable();
                    Table referencedTable = new Table().setName(hibernateReferencedTable.getName());
                    referencedTable.setSchema(hibernateReferencedTable.getCatalog(), hibernateReferencedTable.getSchema());

                    if (hibernateForeignKey.isPhysicalConstraint()) {
                        ForeignKey fk = new ForeignKey();
                        fk.setName(hibernateForeignKey.getName());
                        fk.setPrimaryKeyTable(referencedTable);
                        fk.setForeignKeyTable(currentTable);
                        for (Object column : hibernateForeignKey.getColumns()) {
                            fk.addForeignKeyColumn(new liquibase.structure.core.Column(((org.hibernate.mapping.Column) column).getName()));
                        }
                        for (Object column : hibernateForeignKey.getReferencedColumns()) {
                            fk.addPrimaryKeyColumn(new liquibase.structure.core.Column(((org.hibernate.mapping.Column) column).getName()));
                        }
                        if (fk.getPrimaryKeyColumns() == null || fk.getPrimaryKeyColumns().isEmpty()) {
                            for (Object column : hibernateReferencedTable.getPrimaryKey().getColumns()) {
                                fk.addPrimaryKeyColumn(new liquibase.structure.core.Column(((org.hibernate.mapping.Column) column).getName()));
                            }
                        }

                        LiquibaseForeignKey lfk = resolveLiquibaseForeignKey(metadata, hibernateForeignKey);
                        fk.setDeferrable(lfk.deferrable());
                        fk.setInitiallyDeferred(lfk.initiallyDeferred());

//			Index index = new Index();
//			index.setName("IX_" + fk.getName());
//			index.setTable(fk.getForeignKeyTable());
//			index.setColumns(fk.getForeignKeyColumns());
//			fk.setBackingIndex(index);
//			table.getIndexes().add(index);

                        if (DatabaseObjectComparatorFactory.getInstance().isSameObject(currentTable, table, null, database)) {
                            table.getOutgoingForeignKeys().add(fk);
                            table.getSchema().addDatabaseObject(fk);
                        }
                    }
                }
            }
        }
    }

    private LiquibaseForeignKey resolveLiquibaseForeignKey(MetadataImplementor metadata, org.hibernate.mapping.ForeignKey hibernateForeignKey) {
        Optional<Class> mappedClass = metadata.getEntityBindings().stream()
                .filter(a -> a.getTable() == hibernateForeignKey.getTable())
                .findFirst()
                .map(PersistentClass::getMappedClass);

        return mappedClass.map(clazz -> {
            if (hibernateForeignKey.getColumnSpan() == 1) {
                return resolveLiquibaseForeignKeyFromColumn(hibernateForeignKey, clazz);
            } else {
                return resolveLiquibaseForeignKeyFromTable(clazz);
            }

        }).orElseGet(() -> new LiquibaseForeignKey() {
            @Override
            public boolean deferrable() {
                return false;
            }

            @Override
            public boolean initiallyDeferred() {
                return false;
            }

            @Override
            public Class<? extends Annotation> annotationType() {
                return null;
            }
        });

    }

    private LiquibaseForeignKey resolveLiquibaseForeignKeyFromColumn(org.hibernate.mapping.ForeignKey hibernateForeignKey, Class mappedClass) {

        String fieldName = Optional.of(hibernateForeignKey).map(hfk -> {
            try {
                return hfk.getColumn(0);
            } catch (IndexOutOfBoundsException e) {
                return null;
            }
        }).map(column -> {
            try {
                return (ManyToOne) column.getValue();
            } catch (ClassCastException e) {
                return null;
            }
        }).map(ManyToOne::getPropertyName)
                .orElse(null);

        if (fieldName == null) {
            return null;
        }

        return Optional.ofNullable(mappedClass)
                .map(entityClass -> {
                    try {
                        return entityClass.getDeclaredField(fieldName);
                    } catch (NoSuchFieldException | NullPointerException e) {
                        return null;
                    }
        }).map(c -> c.getAnnotation(LiquibaseForeignKey.class)).orElse(null);

    }

    private LiquibaseForeignKey resolveLiquibaseForeignKeyFromTable(Class mappedClass) {
        return Optional.ofNullable(mappedClass).map(c -> (LiquibaseForeignKey) c.getAnnotation(LiquibaseForeignKey.class)).orElse(null);
                // .filter(d -> hibernateForeignKey.getName().equalsIgnoreCase(d.name()))
    }

}
