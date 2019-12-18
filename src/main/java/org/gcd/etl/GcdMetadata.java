package org.gcd.etl;/**
 * @author jackh
 */

import com.google.common.collect.ImmutableMap;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/**
 * @author jack@indeed.com (Jack Humphrey)
 */
public class GcdMetadata {

    private final Map<Integer, String> countries;
    private final Map<Integer, String> languages;
    private final Map<Integer, String> publicationTypes;
    private final Map<Integer, String> storyTypes;

    private GcdMetadata(final Map<Integer, String> countries,
            final Map<Integer, String> languages,
            final Map<Integer, String> publicationTypes,
            final Map<Integer, String> storyTypes) {
        this.countries = countries;
        this.languages = languages;
        this.publicationTypes = publicationTypes;
        this.storyTypes = storyTypes;
    }

    public Map<Integer, String> getCountryCodeMap() {
        return countries;
    }

    public Map<Integer, String> getLanguageCodeMap() {
        return languages;
    }

    public Map<Integer, String> getPublicationTypeMap() {
        return publicationTypes;
    }

    public Map<Integer, String> getStoryTypeMap() {
        return storyTypes;
    }

    public static class Builder {
        public static GcdMetadata build(final Connection conn) throws SQLException {
            return new GcdMetadata(loadCountries(conn), loadLanguages(conn), loadPublicationTypes(conn), loadStoryTypes(conn));
        }

        private static Map<Integer, String> loadStoryTypes(Connection conn) throws SQLException {
            return getIntegerStringMap(conn, "gcd_story_type", "id", "name");
        }

        private static Map<Integer, String> loadPublicationTypes(Connection conn) throws SQLException {
            return getIntegerStringMap(conn, "gcd_series_publication_type", "id", "name");
        }

        private static Map<Integer, String> loadLanguages(final Connection conn) throws SQLException {
            return getIntegerStringMap(conn, "stddata_language", "id", "code");
        }

        private static Map<Integer, String> loadCountries(final Connection conn) throws SQLException {
            return getIntegerStringMap(conn, "stddata_country", "id", "code");
        }

        private static Map<Integer, String> getIntegerStringMap(Connection conn, final String table, final String keyCol, final String valueCol)
                throws SQLException {
            final ImmutableMap.Builder<Integer, String> builder = new ImmutableMap.Builder<Integer, String>();
            final String query = "SELECT " + keyCol + ", " + valueCol + " FROM " + table;
            final Statement st = conn.createStatement();
            final ResultSet rs = st.executeQuery(query);
            while (rs.next()) {
                builder.put(rs.getInt((keyCol)), rs.getString(valueCol));
            }
            rs.close();
            st.close();
            return builder.build();
        }

    }

}
