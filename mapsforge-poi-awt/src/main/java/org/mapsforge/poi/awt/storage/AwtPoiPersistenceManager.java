/*
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
package org.mapsforge.poi.awt.storage;

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
import org.sqlite.SQLiteConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A {@link PoiPersistenceManager} implementation using a SQLite database via JDBC.
 * <p/>
 * This class can only be used within AWT.
 */
class AwtPoiPersistenceManager extends AbstractPoiPersistenceManager {
    private static final Logger LOGGER = Logger.getLogger(AwtPoiPersistenceManager.class.getName());

    private Connection conn = null;

    private PreparedStatement findCatByIDStatement = null;
    private PreparedStatement findDataByIDStatement = null;
    private PreparedStatement findLocByIDStatement = null;
    private PreparedStatement insertPoiStatementLoc = null;
    private PreparedStatement insertPoiStatementCat = null;
    private PreparedStatement insertPoiStatementData = null;
    private PreparedStatement insertPoiStatementTagKey = null;
    private PreparedStatement insertPoiStatementTagValue = null;
    private PreparedStatement deletePoiStatementLoc = null;
    private PreparedStatement deletePoiStatementCat = null;
    private PreparedStatement deletePoiStatementData = null;
    private PreparedStatement deletePoiStatementTagKey = null;
    private PreparedStatement deletePoiStatementTagValue = null;
    private PreparedStatement isValidDBStatement = null;
    private PreparedStatement metadataStatement = null;

    /**
     * @param dbFilePath Path to SQLite file containing POI data.
     * @param readOnly   If the file does not exist it can be created and filled.
     */
    AwtPoiPersistenceManager(String dbFilePath, boolean readOnly) {
        super();

        // Open / create POI database
        createOrOpenDBFile(dbFilePath, readOnly);

        // Load categories from database
        this.categoryManager = new AwtPoiCategoryManager(this.conn);

        // Queries
        try {
            // Finds a POI-Location by its unique ID
            this.findLocByIDStatement = this.conn.prepareStatement(DbConstants.FIND_LOCATION_BY_ID_STATEMENT);
            // Finds a POI-Data by its unique ID
            this.findCatByIDStatement = this.conn.prepareStatement(DbConstants.FIND_CATEGORIES_BY_ID_STATEMENT);
            // Finds a POI-Categories by its unique ID
            this.findDataByIDStatement = this.conn.prepareStatement(DbConstants.FIND_DATA_BY_ID_STATEMENT);

            // Inserts a POI into index and adds its data
            this.insertPoiStatementLoc = this.conn.prepareStatement(DbConstants.INSERT_INDEX_STATEMENT);
            this.insertPoiStatementData = this.conn.prepareStatement(DbConstants.INSERT_DATA_STATEMENT);
            this.insertPoiStatementTagKey = this.conn.prepareStatement(DbConstants.INSERT_TAGKEY_STATEMENT);
            this.insertPoiStatementTagValue = this.conn.prepareStatement(DbConstants.INSERT_TAGVALUE_STATEMENT);
            this.insertPoiStatementCat = this.conn.prepareStatement(DbConstants.INSERT_CATEGORYMAP_STATEMENT);

            // Deletes a POI given by its ID
            this.deletePoiStatementLoc = this.conn.prepareStatement(DbConstants.DELETE_INDEX_STATEMENT);
            this.deletePoiStatementData = this.conn.prepareStatement(DbConstants.DELETE_DATA_STATEMENT);
            this.deletePoiStatementTagKey = this.conn.prepareStatement(DbConstants.DELETE_OVERHEADTAGKEYS_STATEMENT);
            this.deletePoiStatementTagValue = this.conn.prepareStatement(DbConstants.DELETE_OVERHEADTAGVALUES_STATEMENT);
            this.deletePoiStatementCat = this.conn.prepareStatement(DbConstants.DELETE_CATEGORYMAP_STATEMENT);

            // Metadata
            this.metadataStatement = this.conn.prepareStatement(DbConstants.FIND_METADATA_STATEMENT);
        } catch (SQLException e) {
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
            } catch (SQLException e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.findCatByIDStatement != null) {
            try {
                this.findCatByIDStatement.close();
            } catch (SQLException e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.findDataByIDStatement != null) {
            try {
                this.findDataByIDStatement.close();
            } catch (SQLException e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.insertPoiStatementLoc != null) {
            try {
                this.insertPoiStatementLoc.close();
            } catch (SQLException e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.insertPoiStatementData != null) {
            try {
                this.insertPoiStatementData.close();
            } catch (SQLException e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.insertPoiStatementTagKey != null) {
            try {
                this.insertPoiStatementTagKey.close();
            } catch (SQLException e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.insertPoiStatementTagValue != null) {
            try {
                this.insertPoiStatementTagValue.close();
            } catch (SQLException e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.insertPoiStatementCat != null) {
            try {
                this.insertPoiStatementCat.close();
            } catch (SQLException e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.deletePoiStatementLoc != null) {
            try {
                this.deletePoiStatementLoc.close();
            } catch (SQLException e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.deletePoiStatementCat != null) {
            try {
                this.deletePoiStatementCat.close();
            } catch (SQLException e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.deletePoiStatementData != null) {
            try {
                this.deletePoiStatementData.close();
            } catch (SQLException e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.deletePoiStatementTagKey != null) {
            try {
                this.deletePoiStatementTagKey.close();
            } catch (SQLException e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.deletePoiStatementTagValue != null) {
            try {
                this.deletePoiStatementTagValue.close();
            } catch (SQLException e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.isValidDBStatement != null) {
            try {
                this.isValidDBStatement.close();
            } catch (SQLException e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        if (this.metadataStatement != null) {
            try {
                this.metadataStatement.close();
            } catch (SQLException e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        // Close connection
        if (this.conn != null) {
            try {
                this.conn.close();
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
        try {
            Class.forName("org.sqlite.JDBC");
            SQLiteConfig config = new SQLiteConfig();
            config.setReadOnly(readOnly);
            this.conn = DriverManager.getConnection("jdbc:sqlite:" + dbFilePath, config.toProperties());
            this.conn.setAutoCommit(false);
            this.poiFile = dbFilePath;
        } catch (ClassNotFoundException | SQLException e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }

        // Create file
        if (!isValidDataBase() && !readOnly) {
            try {
                createTables();
            } catch (SQLException e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }
    }

    /**
     * DB open created a new file, so let's create its tables.
     */
    private void createTables() throws SQLException {
        Statement stmt = this.conn.createStatement();

        stmt.execute(DbConstants.DROP_METADATA_STATEMENT);
        stmt.execute(DbConstants.DROP_INDEX_STATEMENT);
        stmt.execute(DbConstants.DROP_DATA_STATEMENT);
        stmt.execute(DbConstants.DROP_TAGKEYS_STATEMENT);
        stmt.execute(DbConstants.DROP_TAGVALUES_STATEMENT);
        stmt.execute(DbConstants.DROP_CATEGORYMAP_STATEMENT);
        stmt.execute(DbConstants.DROP_CATEGORIES_STATEMENT);

        stmt.execute(DbConstants.CREATE_CATEGORIES_STATEMENT);
        stmt.execute(DbConstants.CREATE_CATEGORYMAP_STATEMENT);
        stmt.execute(DbConstants.CREATE_TAGKEYS_STATEMENT);
        stmt.execute(DbConstants.CREATE_TAGVALUES_STATEMENT);
        stmt.execute(DbConstants.CREATE_DATA_STATEMENT);
        stmt.execute(DbConstants.CREATE_INDEX_STATEMENT);
        stmt.execute(DbConstants.CREATE_METADATA_STATEMENT);

        stmt.close();
    }

    private Set<PoiCategory> findCategoriesByID(long poiID) {
        try {
            this.findCatByIDStatement.clearParameters();
            this.findCatByIDStatement.setLong(1, poiID);

            Set<PoiCategory> cats = new HashSet<>();
            ResultSet rs = this.findCatByIDStatement.executeQuery();
            if (rs.next()) {
                long id = rs.getLong(1);
                cats.add(this.categoryManager.getPoiCategoryByID(
                        (int) id));
            }
            rs.close();
            return cats;
        } catch (SQLException | UnknownPoiCategoryException e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }
        return null;
    }

    private Set<Tag> findTagsByID(long poiID) {
        try {
            this.findDataByIDStatement.clearParameters();
            this.findDataByIDStatement.setLong(1, poiID);

            Set<Tag> tags = new HashSet<>();
            ResultSet rs = this.findDataByIDStatement.executeQuery();
            if (rs.next()) {
                tags.add(new Tag(rs.getString(1), rs.getString(2)));
            }
            return tags;
        } catch (SQLException e) {
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
            PreparedStatement stmt = this.conn.prepareStatement(AbstractPoiPersistenceManager.getSQLSelectString(filter, patterns.size()));

            stmt.clearParameters();

            stmt.setDouble(1, bb.maxLatitude);
            stmt.setDouble(2, bb.maxLongitude);
            stmt.setDouble(3, bb.minLatitude);
            stmt.setDouble(4, bb.minLongitude);

            int i = 0; //i is only counted if pattern is not null
            if (patterns != null) {
                Set<Map.Entry<String, String>> entries = patterns.entrySet();
                for (Map.Entry<String, String> pattern : entries) {
                    if (pattern == null) continue;
                    stmt.setString(5 + i, pattern.getKey());
                    stmt.setString(6 + i, pattern.getValue());
                    i += 2;
                }
            }
            stmt.setInt(5 + i, limit);

            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                long id = rs.getLong(1);
                double lat = rs.getDouble(2);
                double lon = rs.getDouble(3);

                this.poi = new PointOfInterest(id, lat, lon, findTagsByID(id), findCategoriesByID(id));
                this.ret.add(this.poi);
            }
            rs.close();
        } catch (SQLException e) {
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
            this.findLocByIDStatement.clearParameters();

            this.findLocByIDStatement.setLong(1, poiID);

            ResultSet rs = this.findLocByIDStatement.executeQuery();
            if (rs.next()) {
                long id = rs.getLong(1);
                double lat = rs.getDouble(2);
                double lon = rs.getDouble(3);

                this.poi = new PointOfInterest(id, lat, lon,
                        findTagsByID(poiID), findCategoriesByID(poiID));

            }
            rs.close();
        } catch (SQLException e) {
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
            ResultSet rs = this.metadataStatement.executeQuery();
            while (rs.next()) {
                String name = rs.getString(1);

                switch (name) {
                    case DbConstants.METADATA_BOUNDS:
                        String bounds = rs.getString(2);
                        if (bounds != null) {
                            poiFileInfoBuilder.bounds = BoundingBox.fromString(bounds);
                        }
                        break;
                    case DbConstants.METADATA_COMMENT:
                        poiFileInfoBuilder.comment = rs.getString(2);
                        break;
                    case DbConstants.METADATA_DATE:
                        poiFileInfoBuilder.date = rs.getLong(2);
                        break;
                    case DbConstants.METADATA_LANGUAGE:
                        poiFileInfoBuilder.language = rs.getString(2);
                        break;
                    case DbConstants.METADATA_VERSION:
                        poiFileInfoBuilder.version = rs.getInt(2);
                        break;
                    case DbConstants.METADATA_WAYS:
                        poiFileInfoBuilder.ways = Boolean.parseBoolean(rs.getString(2));
                        break;
                    case DbConstants.METADATA_WRITER:
                        poiFileInfoBuilder.writer = rs.getString(2);
                        break;
                }
            }
            rs.close();
        } catch (SQLException e) {
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
            this.insertPoiStatementLoc.clearParameters();
            this.insertPoiStatementCat.clearParameters();
            this.insertPoiStatementData.clearParameters();
            this.insertPoiStatementTagKey.clearParameters();
            this.insertPoiStatementTagValue.clearParameters();

            Statement stmt = this.conn.createStatement();
            stmt.execute("BEGIN;");

            for (PointOfInterest poi : pois) {
                this.insertPoiStatementLoc.setLong(1, poi.getId());
                this.insertPoiStatementLoc.setDouble(2, poi.getLatitude());
                this.insertPoiStatementLoc.setDouble(3, poi.getLatitude());
                this.insertPoiStatementLoc.setDouble(4, poi.getLongitude());
                this.insertPoiStatementLoc.setDouble(5, poi.getLongitude());

                //Set multiple poi tags
                Set<Tag> tags = poi.getTags();
                for (Tag tag : tags) {


                    this.insertPoiStatementTagKey.setString(1, tag.key);
                    this.insertPoiStatementTagValue.setString(1, tag.value);
                    this.insertPoiStatementData.setLong(1, poi.getId());
                    this.insertPoiStatementData.setString(2, tag.key);
                    this.insertPoiStatementData.setString(3, tag.value);
                    this.insertPoiStatementData.executeUpdate();

                }
                //Set multiple poi categories
                for (PoiCategory cat : poi.getCategories()) {

                    this.insertPoiStatementCat.setLong(1, poi.getId());
                    this.insertPoiStatementCat.setLong(2, cat.getID());
                    this.insertPoiStatementCat.executeUpdate();
                }

                this.insertPoiStatementLoc.executeUpdate();
            }

            stmt.execute("COMMIT;");
            stmt.close();
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValidDataBase() {
        try {
            this.isValidDBStatement = this.conn.prepareStatement(DbConstants.VALID_DB_STATEMENT);
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }

        // Check for table names
        // TODO Is it necessary to get the tables meta data as well?
        int numTables = 0;
        try {
            ResultSet rs = this.isValidDBStatement.executeQuery();
            if (rs.next()) {
                numTables = rs.getInt(1);
            }
            rs.close();
        } catch (SQLException e) {
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
            this.deletePoiStatementLoc.clearParameters();
            this.deletePoiStatementData.clearParameters();
            this.deletePoiStatementCat.clearParameters();

            Statement stmt = this.conn.createStatement();
            stmt.execute("BEGIN;");

            this.deletePoiStatementLoc.setLong(1, poi.getId());
            this.deletePoiStatementData.setLong(1, poi.getId());
            this.deletePoiStatementCat.setLong(1, poi.getId());

            this.deletePoiStatementLoc.executeUpdate();
            this.deletePoiStatementData.executeUpdate();
            this.deletePoiStatementCat.executeUpdate();

            stmt.execute("COMMIT;");
            stmt.close();
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        }
    }
}
