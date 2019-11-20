package org.gcd.imhotep;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.indeed.flamdex.simple.*;
import com.indeed.flamdex.writer.*;
import org.apache.log4j.BasicConfigurator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import com.indeed.imhotep.archive.SquallArchiveWriter;
import com.indeed.imhotep.archive.compression.SquallArchiveCompressor;
import org.yaml.snakeyaml.Yaml;

public class Main {
    private static final Logger log = Logger.getLogger(Main.class.getName());

    private static final Pattern DATE_PATTERN = Pattern.compile("(\\d\\d\\d\\d)-(\\d\\d)-(\\d\\d)");

    private static final SimpleDateFormat COMPARABLE_DATE_FORMAT = new SimpleDateFormat("yMd");

    public static void main(String[] args) throws ClassNotFoundException, IOException, SQLException, ParseException {
        BasicConfigurator.configure();

        final String configFile = args[0];
        final String date = args[1];
        final String indexName = args[2];

        final long timestampMillis = new SimpleDateFormat("y-M-d z").parse(date +" GMT-06:00").getTime();
        final long timestamp = timestampMillis/1000;

        final Yaml yaml = new Yaml();
        InputStream in = Files.newInputStream(Paths.get(configFile));
        final GcdConfiguration config = yaml.loadAs(in, GcdConfiguration.class);

        log.info("Loading documents from GCD...");
        final Connection conn = getConnection(config.getGcdatabase());
        final GcdMetadata metadata = GcdMetadata.Builder.build(conn);
        final SimpleFlamdexDocWriter writer = getWriter(indexName);
        final int count = loadData(conn, metadata, writer, timestamp);
        conn.close();
        writer.close();
        log.info("Wrote " + count + " documents from GCD database; building sqar...");
        writeSqar(indexName, indexName + ".sqar");
        System.exit(0);
    }

    private static SimpleFlamdexDocWriter getWriter(final String outDir) throws IOException {
        final SimpleFlamdexDocWriter.Config config = new SimpleFlamdexDocWriter.Config();
        config.setDocBufferSize(2000);
        config.setMergeFactor(1000);
        return new SimpleFlamdexDocWriter(outDir, config);
    }

    private static void writeSqar(String indexDir, String outFile) throws IOException {
        Configuration hdfsConf = new Configuration();
        final FileSystem fs = new RawLocalFileSystem();
        fs.setConf(hdfsConf);
        final File shardDir = new File(indexDir);
        final Path outPath = new Path(outFile);
        final SquallArchiveWriter writer =
              new SquallArchiveWriter(fs, outPath, true,
                                      SquallArchiveCompressor.GZIP);
        writer.batchAppendDirectory(shardDir);
        writer.commit();
        log.info("Wrote " + outPath);
    }

    private static Connection getConnection(Gcdatabase config) throws ClassNotFoundException, SQLException {
        final String myDriver = "com.mysql.cj.jdbc.Driver";
        Class.forName(myDriver);
        return DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPassword());
    }

    private static int loadData(final Connection conn, final GcdMetadata metadata, final SimpleFlamdexDocWriter writer, final long unixTime)
            throws SQLException, IOException {
        int count = 0;
        final String query =
                "SELECT \n" +
                "  issue.id AS issue_id,\n" +
                "  issue.number AS issue_number_raw,\n" +
                "  issue.key_date AS pubdateraw, \n" +
                "  issue.price, \n" +
                "  issue.page_count, \n" +
                "  issue.indicia_frequency, \n" +
                "  issue.isbn, \n" +
                "  issue.variant_name, \n" +
                "  issue.variant_of_id AS variant_of_issue_id, \n" +
                "  issue.barcode, \n" +
                "  issue.title, \n" +
                "  issue.on_sale_date AS onsaledateraw, \n" +
                "  issue.rating, \n" +
                "  issue.volume_not_printed, \n" +
                "  issue.editing AS editing, \n" +
                "  issue.notes AS notes, \n" +
                "  UNIX_TIMESTAMP(issue.created) AS created, \n" +
                "  UNIX_TIMESTAMP(issue.modified) AS modified, \n" +
                "  series.id AS series_id, \n" +
                "  series.name AS series_name, \n" +
                "  series.year_began AS series_year_began, \n" +
                "  series.year_ended AS series_year_ended, \n" +
                "  series.is_current AS series_is_current, \n" +
                "  series.country_id AS scountryid, \n" +
                "  series.language_id AS slangid, \n" +
                "  series.has_gallery AS series_has_gallery, \n" +
                "  series.is_comics_publication AS series_is_comics_publication, \n" +
                "  series.color AS series_color, \n" +
                "  series.dimensions AS series_dimensions, \n" +
                "  series.paper_stock AS series_paper_stock, \n" +
                "  series.binding AS series_binding, \n" +
                "  series.publishing_format AS series_publishing_format, \n" +
                "  series.publication_type_id AS spubtypeid, \n" +
                "  series.is_singleton AS series_is_singleton, \n" +
                "  UNIX_TIMESTAMP(series.created) AS series_created, \n" +
                "  UNIX_TIMESTAMP(series.modified) AS series_modified, \n" +
                "  publisher.id AS publisher_id, \n" +
                "  publisher.name AS publisher_name, \n" +
                "  publisher.country_id AS pubcountryid, \n" +
                "  publisher.url AS publisher_url, \n" +
                "  UNIX_TIMESTAMP(publisher.created) AS publisher_created, \n" +
                "  UNIX_TIMESTAMP(publisher.modified) AS publisher_modified, \n" +
                "  indicia.id AS indicia_publisher_id, \n" +
                "  indicia.name AS indicia_publisher_name, \n" +
                "  indicia.country_id AS indpubcountryid, \n" +
                "  indicia.parent_id AS indicia_publisher_parent_id, \n" +
                "  indicia.year_began AS indicia_publisher_year_began, \n" +
                "  indicia.year_ended AS indicia_publisher_year_ended, \n" +
                "  indicia.is_surrogate AS indicia_publisher_is_surrogate, \n" +
                "  indicia.url AS indicia_publisher_url, \n" +
                "  UNIX_TIMESTAMP(indicia.created) AS indicia_publisher_created, \n" +
                "  UNIX_TIMESTAMP(indicia.modified) AS indicia_publisher_modified, \n" +
                "  brand.id AS brand_id, \n" +
                "  brand.name AS brand_name, \n" +
                "  brand.url AS brand_url, \n" +
                "  UNIX_TIMESTAMP(brand.created) AS brand_created, \n" +
                "  UNIX_TIMESTAMP(brand.modified) AS brand_modified, \n" +
                "  story.id AS story_id, \n" +
                "  story.title AS story_title, \n" +
                "  story.feature AS story_feature, \n" +
                "  story.sequence_number AS story_sequence_number, \n" +
                "  story.page_count AS story_page_count, \n" +
                "  story.script AS story_script, \n" +
                "  story.pencils AS story_pencils, \n" +
                "  story.inks AS story_inks, \n" +
                "  story.colors AS story_colors, \n" +
                "  story.letters AS story_letters, \n" +
                "  story.editing AS story_editing, \n" +
                "  story.genre AS story_genre, \n" +
                "  story.characters AS story_characters, \n" +
                "  story.type_id AS strtypeid, \n" +
                "  story.job_number AS story_job_number, \n" +
                "  story.first_line AS story_first_line, \n" +
                "  UNIX_TIMESTAMP(story.created) AS story_created, \n" +
                "  UNIX_TIMESTAMP(story.modified) AS story_modified \n" +
                "FROM gcd_issue AS issue \n" +
                "  INNER JOIN gcd_series AS series ON issue.series_id=series.id \n" +
                "  INNER JOIN gcd_publisher AS publisher ON series.publisher_id=publisher.id\n" +
                "  LEFT OUTER JOIN gcd_indicia_publisher AS indicia ON issue.indicia_publisher_id=indicia.id\n" +
                "  LEFT OUTER JOIN gcd_brand AS brand ON issue.brand_id=brand.id\n" +
                "  LEFT OUTER JOIN gcd_story AS story ON story.issue_id=issue.id";

        final Statement st = conn.createStatement();
        final ResultSet rs = st.executeQuery(query);
        while (rs.next()) {
            try {
                final FlamdexDocument doc = new FlamdexDocument();
                doc.addIntTerm("unixtime", unixTime);
                addInt(rs, doc, "issue_id");
                addOptionalString(rs, doc, "issue_number_raw");
                final String number = rs.getString("issue_number_raw");
                try {
                    doc.addIntTerm("issue_number", Integer.parseInt(number));
                } catch (NumberFormatException e) {
                    // some numbers are not numeric in the data set, don't warn
                }
                addOptionalDate(rs, doc, "pubdateraw", "publication_date");
                addOptionalMultiString(rs, doc, "price", true);
                addOptionalInt(rs, doc, "page_count");
                addOptionalString(rs, doc, "indicia_frequency");
                addOptionalString(rs, doc, "isbn");
                addOptionalString(rs, doc, "variant_name");
                addOptionalInt(rs, doc, "variant_of_issue_id");
                addOptionalString(rs, doc, "barcode");
                addOptionalString(rs, doc, "title");
                addOptionalDate(rs, doc, "onsaledateraw", "on_sale_date");
                addOptionalString(rs, doc, "rating");
                addOptionalInt(rs, doc, "volume_not_printed");
                addOptionalMultiString(rs, doc, "editing", false);
                addOptionalString(rs, doc, "notes");
                addOptionalDateFromTimestamp(rs, doc, "created");
                addOptionalDateFromTimestamp(rs, doc, "modified");
                addInt(rs, doc, "series_id");
                addOptionalString(rs, doc, "series_name");
                addOptionalInt(rs, doc, "series_year_began");
                addOptionalInt(rs, doc, "series_year_ended");
                addOptionalInt(rs, doc, "series_is_current");
                addOptionalStringFromId(rs, doc, "scountryid", "series_country_code", metadata.getCountryCodeMap());
                addOptionalStringFromId(rs, doc, "slangid", "series_language_code", metadata.getLanguageCodeMap());
                addOptionalInt(rs, doc, "series_has_gallery");
                addOptionalInt(rs, doc, "series_is_comics_publication");
                addOptionalString(rs, doc, "series_color");
                addOptionalString(rs, doc, "series_dimensions");
                addOptionalString(rs, doc, "series_paper_stock");
                addOptionalMultiString(rs, doc, "series_binding", true);
                addOptionalString(rs, doc, "series_publishing_format");
                addOptionalStringFromId(rs, doc, "spubtypeid", "series_publication_type", metadata.getPublicationTypeMap());
                addOptionalInt(rs, doc, "series_is_singleton");
                addOptionalDateFromTimestamp(rs, doc, "series_created");
                addOptionalDateFromTimestamp(rs, doc, "series_modified");
                addOptionalInt(rs, doc, "publisher_id");
                addOptionalString(rs, doc, "publisher_name");
                addOptionalStringFromId(rs, doc, "pubcountryid", "publisher_country_code", metadata.getCountryCodeMap());
                addOptionalDateFromTimestamp(rs, doc, "publisher_created");
                addOptionalDateFromTimestamp(rs, doc, "publisher_modified");
                addOptionalString(rs, doc, "publisher_url");
                addOptionalInt(rs, doc, "indicia_publisher_id");
                addOptionalString(rs, doc, "indicia_publisher_name");
                addOptionalStringFromId(rs, doc, "indpubcountryid", "indicia_publisher_country_code",
                        metadata.getCountryCodeMap());
                addOptionalInt(rs, doc, "indicia_publisher_parent_id");
                addOptionalInt(rs, doc, "indicia_publisher_year_began");
                addOptionalInt(rs, doc, "indicia_publisher_year_ended");
                addOptionalInt(rs, doc, "indicia_publisher_is_surrogate");
                addOptionalString(rs, doc, "indicia_publisher_url");
                addOptionalDateFromTimestamp(rs, doc, "indicia_publisher_created");
                addOptionalDateFromTimestamp(rs, doc, "indicia_publisher_modified");
                addOptionalInt(rs, doc, "brand_id");
                addOptionalString(rs, doc, "brand_name");
                addOptionalString(rs, doc, "brand_url");
                addOptionalDateFromTimestamp(rs, doc, "brand_created");
                addOptionalDateFromTimestamp(rs, doc, "brand_modified");
                if (rs.getObject("story_id") != null) {
                    addOptionalInt(rs, doc, "story_id");
                    addOptionalString(rs, doc, "story_title");
                    addOptionalString(rs, doc, "story_feature");
                    addOptionalInt(rs, doc,"story_sequence_number");
                    addOptionalInt(rs, doc,"story_page_count");
                    addOptionalMultiString(rs, doc, "story_script", false);
                    addOptionalMultiString(rs, doc, "story_pencils", false);
                    addOptionalMultiString(rs, doc, "story_inks", false);
                    addOptionalMultiString(rs, doc, "story_colors", false);
                    addOptionalMultiString(rs, doc, "story_letters", false);
                    addOptionalMultiString(rs, doc, "story_editing", false);
                    addOptionalMultiString(rs, doc, "story_genre", false);
                    addOptionalMultiString(rs, doc, "story_characters", false);
                    addOptionalStringFromId(rs, doc, "strtypeid", "story_type", metadata.getStoryTypeMap());
                    addOptionalString(rs, doc, "story_job_number");
                    addOptionalString(rs, doc, "story_first_line");
                    addOptionalDateFromTimestamp(rs, doc, "story_created");
                    addOptionalDateFromTimestamp(rs, doc, "story_modified");
                }

                if (++count % 10000 == 0) {
                    log.info("Processing document #" + count);
                }
                writer.addDocument(doc);
            } catch (SQLException e) {
                log.warning("Skipping document due to: " + e.getMessage());
            }
        }
        rs.close();
        st.close();
        conn.close();
        return count;
    }

    private static String[] addOptionalMultiString(ResultSet rs, FlamdexDocument doc, String field, boolean addRaw) throws SQLException {
        final String value = rs.getString(field);
        if (addRaw) {
            doc.addStringTerm(field + "_raw", value);
        }
        if (value != null) {
            String[] values = value.split("\\s*;\\s*");
            doc.addStringTerms(field, values);
            return values;
        }
        return new String[0];
    }

    private static void addOptionalStringFromId(ResultSet rs, FlamdexDocument doc, String rawField, String field,
            Map<Integer, String> lookupMap) {
        try {
            final int id = rs.getInt(rawField);
            if (lookupMap.containsKey(id)) {
                doc.addStringTerm(field, lookupMap.get(id));
            }
        } catch (SQLException e) {
            log.warning(e.getMessage());
        }
    }

    private static void addOptionalString(final ResultSet rs, final FlamdexDocument doc, final String field) throws SQLException {
        final String value = rs.getString(field);
        if (value != null) {
            doc.addStringTerm(field, value);
        }
    }

    private static void addInt(final ResultSet rs, final FlamdexDocument doc, final String field) throws SQLException {
        doc.addIntTerm(field, rs.getInt(field));
    }

    private static void addOptionalInt(final ResultSet rs, final FlamdexDocument doc, final String field) {
        try {
            doc.addIntTerm(field, rs.getInt(field));
        } catch (SQLException e) {
            log.warning(e.getMessage());
        }
    }

    private static void addOptionalDate(final ResultSet rs, final FlamdexDocument doc, final String rawField, final String field) {
        try {
            final String dateString = rs.getString(rawField);
            final Matcher m = DATE_PATTERN.matcher(dateString);
            if (m.matches()) {
                doc.addIntTerm(field, Integer.parseInt(String.format("%s%s%s", m.group(1), m.group(2), m.group(3))));
            }
        } catch (SQLException e) {
            log.warning(e.getMessage());
        } catch (NumberFormatException e) {
            log.warning(e.getMessage());
        }
    }

    private static void addOptionalDateFromTimestamp(final ResultSet rs, final FlamdexDocument doc, final String field) {
        try {
            final long unixTime = rs.getInt(field);
            if (unixTime > 0) {
                final Date time = new Date(unixTime);
                doc.addIntTerm(field, Integer.parseInt(COMPARABLE_DATE_FORMAT.format(time)));
            }
        } catch (SQLException e) {
            log.warning(e.getMessage());
        }
    }
}
