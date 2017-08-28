/*
 * Copyright 2010, 2011 mapsforge.org
 * Copyright 2010, 2011 Karsten Groll
 * Copyright 2015-2016 devemux86
 *
 * This program is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.mapsforge.poi.android.storage;

import org.mapsforge.core.model.BoundingBox;
import org.mapsforge.core.model.Tag;
import org.mapsforge.poi.storage.AbstractPoiPersistenceManager;
import org.mapsforge.poi.storage.DbConstants;
import org.mapsforge.poi.storage.PoiCategory;
import org.mapsforge.poi.storage.PoiCategoryFilter;
import org.mapsforge.poi.storage.PoiFileInfo;
import org.mapsforge.poi.storage.PoiFileInfoBuilder;
import org.mapsforge.poi.storage.PoiPersistenceManager;
import org.mapsforge.poi.storage.PointOfInterest;
import org.mapsforge.poi.storage.UnknownPoiCategoryException;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import jsqlite.Constants;
import jsqlite.Database;
import jsqlite.Exception;
import jsqlite.Stmt;

/**
 * A {@link PoiPersistenceManager} implementation using a SQLite database via wrapper.
 * <p/>
 * This class can only be used within Android.
 */
class AndroidPoiPersistenceManager extends AbstractPoiPersistenceManager {
    private static final Logger LOGGER = Logger.getLogger(AndroidPoiPersistenceManager.class.getName());

    private Database db = null;

    private Stmt findCatByIDStatement = null;
    private Stmt findDataByIDStatement = null;
    private Stmt findLocByIDStatement = null;
    private Stmt insertPoiStatementLoc = null;
    private Stmt insertPoiStatementCat = null;
    private Stmt insertPoiStatementData = null;
    private Stmt insertPoiStatementTagKey = null;
    private Stmt insertPoiStatementTagValue = null;
    private Stmt deletePoiStatementLoc = null;
    private Stmt deletePoiStatementCat = null;
    private Stmt deletePoiStatementData = null;
    private Stmt deletePoiStatementTagKey = null;
    private Stmt deletePoiStatementTagValue = null;
    private Stmt isValidDBStatement = null;
    private Stmt metadataStatement = null;

    /**
     * @param dbFilePath Path to SQLite file containing POI data.
     * @param readOnly   If the file does not exist it can be created and filled.
     */
    AndroidPoiPersistenceManager(String dbFilePath, boolean readOnly) {
        super();

        // Open / create POI database
        createOrOpenDBFile(dbFilePath, readOnly);

        // Load categories from database
        this.categoryManager = new AndroidPoiCategoryManager(this.db);

        // Queries
        try {
            // Finds a POI-Location by its unique ID
            this.findLocByIDStatement = this.db.prepare(DbConstants.FIND_LOCATION_BY_ID_STATEMENT);
            // Finds a POI-Data by its unique ID
            this.findCatByIDStatement = this.db.prepare(DbConstants.FIND_CATEGORIES_BY_ID_STATEMENT);
            // Finds a POI-Categories by its unique ID
            this.findDataByIDStatement = this.db.prepare(DbConstants.FIND_DATA_BY_ID_STATEMENT);

            // Inserts a POI into index and adds its data
            this.insertPoiStatementLoc = this.db.prepare(DbConstants.INSERT_INDEX_STATEMENT);
            this.insertPoiStatementData = this.db.prepare(DbConstants.INSERT_DATA_STATEMENT);
            this.insertPoiStatementCat = this.db.prepare(DbConstants.INSERT_CATEGORYMAP_STATEMENT);
            this.insertPoiStatementTagKey = this.db.prepare(DbConstants.INSERT_TAGKEY_STATEMENT);
            this.insertPoiStatementTagValue = this.db.prepare(DbConstants.INSERT_TAGVALUE_STATEMENT);

            // Deletes a POI given by its ID
            this.deletePoiStatementLoc = this.db.prepare(DbConstants.DELETE_INDEX_STATEMENT);
            this.deletePoiStatementData = this.db.prepare(DbConstants.DELETE_DATA_STATEMENT);
            this.deletePoiStatementCat = this.db.prepare(DbConstants.DELETE_CATEGORYMAP_STATEMENT);
            this.deletePoiStatementTagKey = this.db.prepare(DbConstants.DELETE_OVERHEADTAGKEYS_STATEMENT);
            this.deletePoiStatementTagValue = this.db.prepare(DbConstants.DELETE_OVERHEADTAGVALUES_STATEMENT);

            // Metadata
            this.metadataStatement = this.db.prepare(DbConstants.FIND_METADATA_STATEMENT);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        // Close statements

        if (this.findLocByIDStatement != null) {
            try {
                this.findLocByIDStatement.close();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.findCatByIDStatement != null) {
            try {
                this.findCatByIDStatement.close();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.findDataByIDStatement != null) {
            try {
                this.findDataByIDStatement.close();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.insertPoiStatementLoc != null) {
            try {
                this.insertPoiStatementLoc.close();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.insertPoiStatementData != null) {
            try {
                this.insertPoiStatementData.close();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.insertPoiStatementTagKey != null) {
            try {
                this.insertPoiStatementTagKey.close();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.insertPoiStatementTagValue != null) {
            try {
                this.insertPoiStatementTagValue.close();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.insertPoiStatementCat != null) {
            try {
                this.insertPoiStatementCat.close();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.deletePoiStatementLoc != null) {
            try {
                this.deletePoiStatementLoc.close();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.deletePoiStatementCat != null) {
            try {
                this.deletePoiStatementCat.close();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.deletePoiStatementData != null) {
            try {
                this.deletePoiStatementData.close();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.deletePoiStatementTagKey != null) {
            try {
                this.deletePoiStatementTagKey.close();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.deletePoiStatementTagValue != null) {
            try {
                this.deletePoiStatementTagValue.close();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.isValidDBStatement != null) {
            try {
                this.isValidDBStatement.close();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.metadataStatement != null) {
            try {
                this.metadataStatement.close();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        // Close connection
        if (this.db != null) {
            try {
                this.db.close();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        this.poiFile = null;
    }

    /**
     * @param dbFilePath Path to SQLite file containing POI data.
     * @param readOnly   If the file does not exist it can be created and filled.
     */
    private void createOrOpenDBFile(String dbFilePath, boolean readOnly) {
        // Open file
        this.db = new Database();
        try {
            this.db.open(dbFilePath, readOnly ? Constants.SQLITE_OPEN_READONLY : Constants.SQLITE_OPEN_READWRITE | Constants.SQLITE_OPEN_CREATE);
            this.poiFile = dbFilePath;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }

        // Create file
        if (!isValidDataBase() && !readOnly) {
            try {
                createTables();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }
    }

    /**
     * DB open created a new file, so let's create its tables.
     */
    private void createTables() throws Exception {
        this.db.exec(DbConstants.DROP_METADATA_STATEMENT, null);
        this.db.exec(DbConstants.DROP_INDEX_STATEMENT, null);
        this.db.exec(DbConstants.DROP_DATA_STATEMENT, null);
        this.db.exec(DbConstants.DROP_TAGKEYS_STATEMENT, null);
        this.db.exec(DbConstants.DROP_TAGVALUES_STATEMENT, null);
        this.db.exec(DbConstants.DROP_CATEGORYMAP_STATEMENT, null);
        this.db.exec(DbConstants.DROP_CATEGORIES_STATEMENT, null);

        this.db.exec(DbConstants.CREATE_CATEGORIES_STATEMENT, null);
        this.db.exec(DbConstants.CREATE_CATEGORYMAP_STATEMENT, null);
        this.db.exec(DbConstants.CREATE_TAGKEYS_STATEMENT, null);
        this.db.exec(DbConstants.CREATE_TAGVALUES_STATEMENT, null);
        this.db.exec(DbConstants.CREATE_DATA_STATEMENT, null);
        this.db.exec(DbConstants.CREATE_INDEX_STATEMENT, null);
        this.db.exec(DbConstants.CREATE_METADATA_STATEMENT, null);
    }

    private Set<PoiCategory> findCategoriesByID(long poiID) {
        try {
            this.findCatByIDStatement.reset();
            this.findCatByIDStatement.clear_bindings();
            this.findCatByIDStatement.bind(1, poiID);

            Set<PoiCategory> cats = new HashSet<>();
            while (findCatByIDStatement.step()) {
                cats.add(this.categoryManager.getPoiCategoryByID(
                        findCatByIDStatement.column_int(1)));
            }
            return cats;
        } catch (Exception | UnknownPoiCategoryException e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }
        return null;
    }

    private Set<Tag> findTagsByID(long poiID) {
        try {
            this.findDataByIDStatement.reset();
            this.findDataByIDStatement.clear_bindings();
            this.findDataByIDStatement.bind(1, poiID);

            Set<Tag> tags = new HashSet<>();
            while (findDataByIDStatement.step()) {
                tags.add(new Tag(findDataByIDStatement.column_string(1)
                        , findDataByIDStatement.column_string(2)));
            }
            return tags;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<PointOfInterest> findInRect(BoundingBox bb, PoiCategoryFilter filter,
                                                  Map<String, String> patterns, int limit) {
        // Clear previous results
        this.ret.clear();

        // Query
        try {
            Stmt stmt = this.db.prepare(AbstractPoiPersistenceManager.getSQLSelectString(filter, patterns.size()));

            stmt.reset();
            stmt.clear_bindings();

            stmt.bind(1, bb.maxLatitude);
            stmt.bind(2, bb.maxLongitude);
            stmt.bind(3, bb.minLatitude);
            stmt.bind(4, bb.minLongitude);

            int i = 0; //i is only counted if pattern is not null
            if (patterns != null) {
                Set<Map.Entry<String, String>> entries = patterns.entrySet();
                for (Map.Entry<String, String> pattern : entries) {
                    if (pattern == null) continue;
                    stmt.bind(5 + i, pattern.getKey());
                    stmt.bind(6 + i, pattern.getValue());
                    i += 2;
                }
            }
            stmt.bind(5 + i, limit);

            while (stmt.step()) {
                long id = stmt.column_long(0);
                double lat = stmt.column_double(1);
                double lon = stmt.column_double(2);

                this.poi = new PointOfInterest(id, lat, lon, findTagsByID(id), findCategoriesByID(id));
                this.ret.add(this.poi);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }

        return this.ret;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PointOfInterest findPointByID(long poiID) {
        // Clear previous results
        this.poi = null;

        // Query
        try {
            this.findLocByIDStatement.reset();
            this.findLocByIDStatement.clear_bindings();
            this.findLocByIDStatement.bind(1, poiID);

            if (!this.findLocByIDStatement.step()) return null;
            double lat = this.findLocByIDStatement.column_double(1);
            double lon = this.findLocByIDStatement.column_double(2);

            this.poi = new PointOfInterest(poiID, lat, lon, findTagsByID(poiID), findCategoriesByID(poiID));
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }

        return this.poi;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PoiFileInfo getPoiFileInfo() {
        PoiFileInfoBuilder poiFileInfoBuilder = new PoiFileInfoBuilder();

        // Query
        try {
            while (this.metadataStatement.step()) {
                String name = this.metadataStatement.column_string(0);

                switch (name) {
                    case DbConstants.METADATA_BOUNDS:
                        String bounds = this.metadataStatement.column_string(1);
                        if (bounds != null) {
                            poiFileInfoBuilder.bounds = BoundingBox.fromString(bounds);
                        }
                        break;
                    case DbConstants.METADATA_COMMENT:
                        poiFileInfoBuilder.comment = this.metadataStatement.column_string(1);
                        break;
                    case DbConstants.METADATA_DATE:
                        poiFileInfoBuilder.date = this.metadataStatement.column_long(1);
                        break;
                    case DbConstants.METADATA_LANGUAGE:
                        poiFileInfoBuilder.language = this.metadataStatement.column_string(1);
                        break;
                    case DbConstants.METADATA_VERSION:
                        poiFileInfoBuilder.version = this.metadataStatement.column_int(1);
                        break;
                    case DbConstants.METADATA_WAYS:
                        poiFileInfoBuilder.ways = Boolean.parseBoolean(this.metadataStatement.column_string(1));
                        break;
                    case DbConstants.METADATA_WRITER:
                        poiFileInfoBuilder.writer = this.metadataStatement.column_string(1);
                        break;
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }

        return poiFileInfoBuilder.build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void insertPointOfInterest(PointOfInterest poi) {
        Collection<PointOfInterest> c = new HashSet<>();
        c.add(poi);
        insertPointsOfInterest(c);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void insertPointsOfInterest(Collection<PointOfInterest> pois) {
        try {
            this.db.exec("BEGIN;", null);

            for (PointOfInterest poi : pois) {
                this.insertPoiStatementLoc.reset();
                this.insertPoiStatementLoc.clear_bindings();

                this.insertPoiStatementLoc.bind(1, poi.getId());
                this.insertPoiStatementLoc.bind(2, poi.getLatitude());
                this.insertPoiStatementLoc.bind(3, poi.getLatitude());
                this.insertPoiStatementLoc.bind(4, poi.getLongitude());
                this.insertPoiStatementLoc.bind(5, poi.getLongitude());

                //Set multiple poi tags
                Set<Tag> tags = poi.getTags();
                for (Tag tag : tags) {
                    this.insertPoiStatementData.reset();
                    this.insertPoiStatementData.clear_bindings();

                    this.insertPoiStatementTagKey.bind(1, tag.key);
                    this.insertPoiStatementTagValue.bind(1, tag.value);
                    this.insertPoiStatementData.bind(1, poi.getId());
                    this.insertPoiStatementData.bind(2, tag.key);
                    this.insertPoiStatementData.bind(3, tag.value);
                    this.insertPoiStatementTagKey.step();
                    this.insertPoiStatementTagValue.step();
                    this.insertPoiStatementData.step();

                }
                //Set multiple poi categories
                for (PoiCategory cat : poi.getCategories()) {
                    this.insertPoiStatementCat.reset();
                    this.insertPoiStatementCat.clear_bindings();

                    this.insertPoiStatementCat.bind(1, poi.getId());
                    this.insertPoiStatementCat.bind(2, cat.getID());
                    this.insertPoiStatementCat.step();
                }

                this.insertPoiStatementLoc.step();
            }

            this.db.exec("COMMIT;", null);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValidDataBase() {
        try {
            this.isValidDBStatement = this.db.prepare(DbConstants.VALID_DB_STATEMENT);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }

        // Check for table names
        // TODO Is it necessary to get the tables meta data as well?
        int numTables = 0;
        try {
            if (this.isValidDBStatement.step()) {
                numTables = this.isValidDBStatement.column_int(0);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }

        return numTables == DbConstants.NUMBER_OF_TABLES;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removePointOfInterest(PointOfInterest poi) {
        try {
            this.deletePoiStatementLoc.reset();
            this.deletePoiStatementLoc.clear_bindings();
            this.deletePoiStatementCat.reset();
            this.deletePoiStatementCat.clear_bindings();
            this.deletePoiStatementData.reset();
            this.deletePoiStatementData.clear_bindings();

            this.db.exec("BEGIN;", null);

            this.deletePoiStatementLoc.bind(1, poi.getId());
            this.deletePoiStatementCat.bind(1, poi.getId());
            this.deletePoiStatementData.bind(1, poi.getId());

            this.deletePoiStatementLoc.step();
            this.deletePoiStatementCat.step();
            this.deletePoiStatementData.step();

            this.db.exec("COMMIT;", null);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }
    }
}
