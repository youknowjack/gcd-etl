package org.gcd.etl;

import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.yaml.snakeyaml.Yaml;

import com.indeed.flamdex.simple.*;
import com.indeed.flamdex.writer.*;
import com.indeed.imhotep.archive.SquallArchiveWriter;
import com.indeed.imhotep.archive.compression.SquallArchiveCompressor;

public class Main {
    private static final Logger log = Logger.getLogger(Main.class);

    private static final Pattern DATE_PATTERN = Pattern.compile("(\\d\\d\\d\\d)-(\\d\\d)-(\\d\\d)");

    private static final SimpleDateFormat COMPARABLE_DATE_FORMAT = new SimpleDateFormat("yMMdd");

    public static void main(String[] args) throws ClassNotFoundException, IOException, SQLException, ParseException {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);

        final String configFile = args[0];
        final String date = args[1];
        final String indexName = args[2];
        final OutputType outputType = OutputType.valueOf(args[3]);

        final long timestampMillis = new SimpleDateFormat("y-M-d z").parse(date +" GMT-06:00").getTime();
        final long timestamp = timestampMillis/1000;

        final Yaml yaml = new Yaml();
        InputStream in = Files.newInputStream(Paths.get(configFile));
        final GcdConfiguration config = yaml.loadAs(in, GcdConfiguration.class);

        log.info("Loading documents from GCD...");
        final Connection conn = getConnection(config.getGcdatabase());
        final GcdSchema schema = config.getGcdatabase().getGcdSchema();
        final GcdMetadata metadata = GcdMetadata.Builder.build(conn, schema);
        final Map<Long, GcdStoryCredit> storyCredits;
        if (schema.isStoryCredit()) {
            storyCredits = GcdStoryCredit.loadAllStoryCredits(conn);
            log.info("Loaded credit details for " + storyCredits.size() + " stories");
        } else {
            storyCredits = new ImmutableMap.Builder<Long, GcdStoryCredit>().build();
        }
        switch (outputType) {
            case FLAMDEX:
                final SimpleFlamdexDocWriter flamdexWriter = getFlamdexWriter(indexName);
                final int fcount = extractDataToFlamdex(conn, metadata, schema, storyCredits, flamdexWriter, timestamp);
                conn.close();
                flamdexWriter.close();
                log.info("Wrote " + fcount + " documents from GCD database; building sqar...");
                writeSqar(indexName, indexName + ".sqar");
                break;

            case PARQUET:
                final int pcount = extractDataToParquet(conn, metadata, schema, storyCredits, date, indexName, timestamp);
                conn.close();
                log.info("Wrote " + pcount + " documents from GCD database");
                break;
        }
        System.exit(0);
    }

    private static ParquetWriter getParquetWriter(String indexName, int snapshot, int part) throws IOException {
        final Schema avroSchema = GcdIssueData.getClassSchema();
        final MessageType parquetSchema = new AvroSchemaConverter().convert(avroSchema);
        final String fmtPart = String.format("%04d", part);
        final Path filePath = new Path("./" + indexName + "/snapshot=" + snapshot + "/part-" + fmtPart + ".parquet");
        return AvroParquetWriter.builder(filePath)
                .withSchema(avroSchema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build();
    }

    private static SimpleFlamdexDocWriter getFlamdexWriter(final String outDir) throws IOException {
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

    private static final String GCD_QUERY =
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

    private static String getGcdQuery(final GcdSchema schema) {
        String query = GCD_QUERY;
        if (!schema.isPublicationType()) {
            query = query.replace("series.publication_type_id AS spubtypeid,", "");
        }
        if (!schema.isVolumeNotPrinted()) {
            query = query.replace("issue.volume_not_printed,", "");
        }
        if (!schema.isSeriesIsSingleton()) {
            query = query.replace("series.is_singleton AS series_is_singleton,", "");
        }
        if (!schema.isStoryFirstLine()) {
            query = query.replace("story.first_line AS story_first_line,", "");
        }
        return query;
    }

    private static int extractDataToFlamdex(
            final Connection conn,
            final GcdMetadata metadata,
            final GcdSchema schema,
            final Map<Long, GcdStoryCredit> storyCredits,
            final SimpleFlamdexDocWriter writer,
            final long unixTime
    )
            throws SQLException, IOException {
        int count = 0;
        final Statement st = conn.createStatement();
        final ResultSet rs = st.executeQuery(getGcdQuery(schema));
        while (rs.next()) {
            try {
                final FlamdexDocument doc = new FlamdexDocument();
                doc.addIntTerm("unixtime", unixTime);
                addLong(rs, "issue_id", id -> doc.addIntTerm("issue_id", id));
                addOptionalString(rs, "issue_number_raw", num -> doc.addStringTerm("issue_number_raw", num));
                final String number = rs.getString("issue_number_raw");
                try {
                    doc.addIntTerm("issue_number", Integer.parseInt(number));
                } catch (NumberFormatException e) {
                    // some numbers are not numeric in the data set, don't warn
                }
                addOptionalDate(rs, "pubdateraw", date -> doc.addIntTerm("publication_date", date));
                addOptionalString(rs, "price", price -> doc.addStringTerm("price_raw", price));
                addOptionalMultiString(rs, "price", prices -> doc.addStringTerms("price", prices));
                addIntWithDefault(rs, "page_count", -1, pageCount -> doc.addIntTerm("page_count", pageCount));
                addOptionalString(rs, "indicia_frequency", freq -> doc.addStringTerm("indicia_frequency", freq));
                addOptionalString(rs, "isbn", isbn -> doc.addStringTerm("isbn", isbn));
                addOptionalString(rs, "variant_name", name -> doc.addStringTerm("variant_name", name));
                addOptionalLong(rs, "variant_of_issue_id", variant_id -> doc.addIntTerm("variant_of_issue_id", variant_id));
                addOptionalString(rs, "barcode", barcode -> doc.addStringTerm("barcode", barcode));
                addOptionalString(rs, "title", title -> doc.addStringTerm("title", title));
                addOptionalDate(rs, "onsaledateraw", date -> doc.addIntTerm("on_sale_date", date));
                addOptionalString(rs, "rating", rating -> doc.addStringTerm("rating", rating));
                if (schema.isVolumeNotPrinted()) {
                    addOptionalInt(rs, "volume_not_printed", notprinted -> doc.addIntTerm("volume_not_printed", notprinted));
                }
                addOptionalMultiString(rs, "editing", editors -> doc.addStringTerms("editing", editors));
                addOptionalString(rs, "notes", notes -> doc.addStringTerm("notes", notes));
                addOptionalDateFromTimestamp(rs, "created", created -> doc.addIntTerm("created", created));
                addOptionalDateFromTimestamp(rs, "modified", modified -> doc.addIntTerm("modified", modified));
                addLong(rs, "series_id", id -> doc.addIntTerm("series_id", id));
                addOptionalString(rs, "series_name", name -> doc.addStringTerm("series_name", name));
                addIntWithDefault(rs, "series_year_began", -1, began -> doc.addIntTerm("series_year_began", began));
                addIntWithDefault(rs, "series_year_ended", -1, ended -> doc.addIntTerm("series_year_ended", ended));
                addOptionalInt(rs, "series_is_current", iscur -> doc.addIntTerm("series_is_current", iscur));
                addOptionalStringFromId(rs, "scountryid", metadata.getCountryCodeMap(), code -> doc.addStringTerm("series_country_code", code));
                addOptionalStringFromId(rs, "slangid", metadata.getLanguageCodeMap(), code -> doc.addStringTerm("series_language_code", code));
                addOptionalInt(rs, "series_has_gallery", hasgal -> doc.addIntTerm("series_has_gallery", hasgal));
                addOptionalInt(rs, "series_is_comics_publication", iscomics -> doc.addIntTerm("series_is_comics_publication", iscomics));
                addOptionalString(rs, "series_color", color -> doc.addStringTerm("series_color", color));
                addOptionalString(rs, "series_dimensions", dim -> doc.addStringTerm("series_dimensions", dim));
                addOptionalString(rs, "series_paper_stock", stock -> doc.addStringTerm("series_paper_stock", stock));
                addOptionalMultiString(rs, "series_binding", bindings -> doc.addStringTerms("series_binding", bindings));
                addOptionalString(rs, "series_publishing_format", fmt -> doc.addStringTerm("series_publishing_format", fmt));
                if (schema.isPublicationType()) {
                    addOptionalStringFromId(rs, "spubtypeid", metadata.getPublicationTypeMap(), type -> doc.addStringTerm("series_publication_type", type));
                }
                if (schema.isSeriesIsSingleton()) {
                    addOptionalInt(rs, "series_is_singleton", sing -> doc.addIntTerm("series_is_singleton", sing));
                }
                addOptionalDateFromTimestamp(rs, "series_created", created -> doc.addIntTerm("series_created", created));
                addOptionalDateFromTimestamp(rs, "series_modified", modified -> doc.addIntTerm("series_modified", modified));
                addOptionalLong(rs, "publisher_id", id -> doc.addIntTerm("publisher_id", id));
                addOptionalString(rs, "publisher_name", name -> doc.addStringTerm("publisher_name", name));
                addOptionalStringFromId(rs, "pubcountryid", metadata.getCountryCodeMap(), code -> doc.addStringTerm("publisher_country_code", code));
                addOptionalDateFromTimestamp(rs, "publisher_created", created -> doc.addIntTerm("publisher_created", created));
                addOptionalDateFromTimestamp(rs, "publisher_modified", modified -> doc.addIntTerm("publisher_modified", modified));
                addOptionalString(rs, "publisher_url", url -> doc.addStringTerm("publisher_url", url));
                addOptionalLong(rs, "indicia_publisher_id", id -> doc.addIntTerm("indicia_publisher_id", id));
                addOptionalString(rs, "indicia_publisher_name", name -> doc.addStringTerm("indicia_publisher_name", name));
                addOptionalStringFromId(rs, "indpubcountryid", metadata.getCountryCodeMap(), code -> doc.addStringTerm("indicia_publisher_country_code", code));
                addOptionalLong(rs, "indicia_publisher_parent_id", id -> doc.addIntTerm("indicia_publisher_parent_id", id));
                addIntWithDefault(rs, "indicia_publisher_year_began", -1, began -> doc.addIntTerm("indicia_publisher_year_began", began));
                addIntWithDefault(rs, "indicia_publisher_year_ended", -1, ended -> doc.addIntTerm("indicia_publisher_year_ended", ended));
                addOptionalInt(rs, "indicia_publisher_is_surrogate", sur -> doc.addIntTerm("indicia_publisher_is_surrogate", sur));
                addOptionalString(rs, "indicia_publisher_url", url -> doc.addStringTerm("indicia_publisher_url", url));
                addOptionalDateFromTimestamp(rs, "indicia_publisher_created", created -> doc.addIntTerm("indicia_publisher_created", created));
                addOptionalDateFromTimestamp(rs, "indicia_publisher_modified", modified -> doc.addIntTerm("indicia_publisher_modified", modified));
                addOptionalLong(rs, "brand_id", id -> doc.addIntTerm("brand_id", id));
                addOptionalString(rs, "brand_name", name -> doc.addStringTerm("brand_name", name));
                addOptionalString(rs, "brand_url", url -> doc.addStringTerm("brand_url", url));
                addOptionalDateFromTimestamp(rs, "brand_created", created -> doc.addIntTerm("brand_created", created));
                addOptionalDateFromTimestamp(rs, "brand_modified", modified -> doc.addIntTerm("brand_modified", modified));
                if (rs.getObject("story_id") != null) {
                    addOptionalLong(rs, "story_id", id -> doc.addIntTerm("story_id", id));
                    addOptionalString(rs, "story_title", title -> doc.addStringTerm("story_title", title));
                    addOptionalString(rs, "story_feature", feature -> doc.addStringTerm("story_feature", feature));
                    addIntWithDefault(rs, "story_sequence_number", -1, num -> doc.addIntTerm("story_sequence_number", num));
                    addIntWithDefault(rs, "story_page_count", -1, pageCount -> doc.addIntTerm("story_page_count", pageCount));
                    final long storyId = rs.getLong("story_id");
                    final GcdStoryCredit credit = storyCredits.get(storyId);
                    if (credit != null) {
                        addOptionalCredit(credit,
                                (field, terms) -> doc.addStringTerms(field, terms), (field, terms) -> doc.addIntTerms(field, terms));

                        doc.addStringTerm("story_credit_source", "gcd_story_credit");
                    } else {
                        addOptionalMultiString(rs, "story_script", scriptors -> doc.addStringTerms("story_script", scriptors));
                        addOptionalMultiString(rs, "story_pencils", artists -> doc.addStringTerms("story_pencils", artists));
                        addOptionalMultiString(rs, "story_inks", inkers -> doc.addStringTerms("story_inks", inkers));
                        addOptionalMultiString(rs, "story_colors", colorists -> doc.addStringTerms("story_colors", colorists));
                        addOptionalMultiString(rs, "story_letters", letterers -> doc.addStringTerms("story_letters", letterers));
                        addOptionalMultiString(rs, "story_editing", editors -> doc.addStringTerms("story_editing", editors));

                        doc.addStringTerm("story_credit_source", "gcd_story");
                    }
                    addOptionalMultiString(rs, "story_genre", genres -> doc.addStringTerms("story_genre", genres));
                    addOptionalMultiString(rs, "story_characters", characters -> doc.addStringTerms("story_characters", characters));
                    addOptionalStringFromId(rs, "strtypeid", metadata.getStoryTypeMap(), type -> doc.addStringTerm("story_type", type));
                    addOptionalString(rs, "story_job_number", num -> doc.addStringTerm("story_job_number", num));
                    if (schema.isStoryFirstLine()) {
                        addOptionalString(rs, "story_first_line", line -> doc.addStringTerm("story_first_line", line));
                    }
                    addOptionalDateFromTimestamp(rs, "story_created", created -> doc.addIntTerm("story_created", created));
                    addOptionalDateFromTimestamp(rs, "story_modified", modified -> doc.addIntTerm("story_modified", modified));
                }

                if (++count % 10000 == 0) {
                    log.info("Processing document #" + count);
                }
                writer.addDocument(doc);
            } catch (SQLException e) {
                log.warn("Skipping document due to: " + e.getMessage());
            }
        }
        rs.close();
        st.close();
        conn.close();
        return count;
    }

    private static int extractDataToParquet(
            final Connection conn,
            final GcdMetadata metadata,
            final GcdSchema schema,
            Map<Long, GcdStoryCredit> storyCredits,
            final String date,
            final String indexName,
            final long unixTime
    )
            throws SQLException, IOException {
        int count = 0;
        final Statement st = conn.createStatement();
        final ResultSet rs = st.executeQuery(getGcdQuery(schema));
        int part = 0;
        final int snapshot = Integer.parseInt(date.replaceAll("-", ""));
        ParquetWriter writer = getParquetWriter(indexName, snapshot, part);
        while (rs.next()) {
            try {
                final GcdIssueData doc = new GcdIssueData();
                doc.setUnixTime(unixTime);
                addLong(rs, "issue_id", id -> doc.setIssueId(id));
                addOptionalString(rs, "issue_number_raw", num -> doc.setIssueNumberRaw(num));
                final String number = rs.getString("issue_number_raw");
                try {
                    doc.setIssueNumber(Integer.parseInt(number));
                } catch (NumberFormatException e) {
                    // some numbers are not numeric in the data set, don't warn
                }
                addOptionalDate(rs, "pubdateraw", pubdate -> doc.setPublicationDate(pubdate));
                addOptionalString(rs, "price", price -> doc.setPriceRaw(price));
                addOptionalMultiString(rs, "price", prices -> doc.setPrice(prices));
                addOptionalInt(rs, "page_count", pageCount -> doc.setPageCount(pageCount));
                addOptionalString(rs, "indicia_frequency", freq -> doc.setIndiciaFrequency(freq));
                addOptionalString(rs, "isbn", isbn -> doc.setIsbn(isbn));
                addOptionalString(rs, "variant_name", name -> doc.setVariantName(name));
                addOptionalLong(rs, "variant_of_issue_id", vid -> doc.setVariantOfIssueId(vid));
                addOptionalString(rs, "barcode", barcode -> doc.setBarcode(barcode));
                addOptionalString(rs, "title", title -> doc.setTitle(title));
                addOptionalDate(rs, "onsaledateraw", osdate -> doc.setOnSaleDate(osdate));
                addOptionalString(rs, "rating", rating -> doc.setRating(rating));
                if (schema.isVolumeNotPrinted()) {
                    addOptionalInt(rs, "volume_not_printed", vnp -> doc.setVolumeNotPrinted(vnp == 1));
                }
                addOptionalMultiString(rs, "editing", editors -> doc.setEditing(editors));
                addOptionalString(rs, "notes", notes -> doc.setNotes(notes));
                addOptionalDateFromTimestamp(rs, "created", created -> doc.setCreated(created));
                addOptionalDateFromTimestamp(rs, "modified", modified -> doc.setModified(modified));
                addLong(rs, "series_id", id -> doc.setSeriesId(id));
                addOptionalString(rs, "series_name", name -> doc.setSeriesName(name));
                addOptionalInt(rs, "series_year_began", began -> doc.setSeriesYearBegan(began));
                addOptionalInt(rs, "series_year_ended", ended -> doc.setSeriesYearEnded(ended));
                addOptionalInt(rs, "series_is_current", cur -> doc.setSeriesIsCurrent(cur == 1));
                addOptionalStringFromId(rs, "scountryid", metadata.getCountryCodeMap(), code -> doc.setSeriesCountryCode(code));
                addOptionalStringFromId(rs, "slangid", metadata.getLanguageCodeMap(), code -> doc.setSeriesLanguageCode(code));
                addOptionalInt(rs, "series_has_gallery", hasgal -> doc.setSeriesHasGallery(hasgal == 1));
                addOptionalInt(rs, "series_is_comics_publication", ispub -> doc.setSeriesIsComicsPublication(ispub == 1));
                addOptionalString(rs, "series_color", color -> doc.setSeriesColor(color));
                addOptionalString(rs, "series_dimensions", dim -> doc.setSeriesDimensions(dim));
                addOptionalString(rs, "series_paper_stock", stock -> doc.setSeriesPaperStock(stock));
                addOptionalMultiString(rs, "series_binding", bindings -> doc.setSeriesBinding(bindings));
                addOptionalString(rs, "series_publishing_format", fmt -> doc.setSeriesPublishingFormat(fmt));
                if (schema.isPublicationType()) {
                    addOptionalStringFromId(rs, "spubtypeid", metadata.getPublicationTypeMap(), type -> doc.setSeriesPublishingType(type));
                }
                if (schema.isSeriesIsSingleton()) {
                    addOptionalInt(rs, "series_is_singleton", sing -> doc.setSeriesIsSingleton(sing == 1));
                }
                addOptionalDateFromTimestamp(rs, "series_created", created -> doc.setSeriesCreated(created));
                addOptionalDateFromTimestamp(rs, "series_modified", modified -> doc.setSeriesModified(modified));
                addOptionalLong(rs, "publisher_id", id -> doc.setPublisherId(id));
                addOptionalString(rs, "publisher_name", name -> doc.setPublisherName(name));
                addOptionalStringFromId(rs, "pubcountryid", metadata.getCountryCodeMap(), code -> doc.setPublisherCountryCode(code));
                addOptionalDateFromTimestamp(rs, "publisher_created", created -> doc.setPublisherCreated(created));
                addOptionalDateFromTimestamp(rs, "publisher_modified", modified -> doc.setPublisherModified(modified));
                addOptionalString(rs, "publisher_url", url -> doc.setPublisherUrl(url));
                addOptionalLong(rs, "indicia_publisher_id", id -> doc.setIndiciaPublisherId(id));
                addOptionalString(rs, "indicia_publisher_name", name -> doc.setIndiciaPublisherName(name));
                addOptionalStringFromId(rs, "indpubcountryid", metadata.getCountryCodeMap(), code -> doc.setIndiciaPublisherCountryCode(code));
                addOptionalLong(rs, "indicia_publisher_parent_id", id -> doc.setIndiciaPublisherParentId(id));
                addOptionalInt(rs, "indicia_publisher_year_began", began -> doc.setIndiciaPublisherYearBegan(began));
                addOptionalInt(rs, "indicia_publisher_year_ended", ended -> doc.setIndiciaPublisherYearEnded(ended));
                addOptionalInt(rs, "indicia_publisher_is_surrogate", sur -> doc.setIndiciaPublisherIsSurrogate(sur == 1));
                addOptionalString(rs, "indicia_publisher_url", url -> doc.setIndiciaPublisherUrl(url));
                addOptionalDateFromTimestamp(rs, "indicia_publisher_created", created -> doc.setIndiciaPublisherCreated(created));
                addOptionalDateFromTimestamp(rs, "indicia_publisher_modified", modified -> doc.setIndiciaPublisherModified(modified));
                addOptionalLong(rs, "brand_id", id -> doc.setBrandId(id));
                addOptionalString(rs, "brand_name", name -> doc.setBrandName(name));
                addOptionalString(rs, "brand_url", url -> doc.setBrandUrl(url));
                addOptionalDateFromTimestamp(rs, "brand_created", created -> doc.setBrandCreated(created));
                addOptionalDateFromTimestamp(rs, "brand_modified", modified -> doc.setBrandModified(modified));
                if (rs.getObject("story_id") != null) {
                    addOptionalLong(rs, "story_id", id -> doc.setStoryId(id));
                    addOptionalString(rs, "story_title", title -> doc.setStoryTitle(title));
                    addOptionalString(rs, "story_feature", feature -> doc.setStoryFeature(feature));
                    addOptionalInt(rs, "story_sequence_number", num -> doc.setStorySequenceNumber(num));
                    addOptionalInt(rs, "story_page_count", pageCount -> doc.setStoryPageCount(pageCount));
                    final long storyId = rs.getLong("story_id");
                    final GcdStoryCredit credit = storyCredits.get(storyId);
                    if (credit != null) {
                        doc.setStoryScript(credit.getNames(GcdStoryCredit.CreditType.SCRIPT));
                        doc.setStoryScriptCreatorId(credit.getIds(GcdStoryCredit.CreditType.SCRIPT));
                        doc.setStoryPencils(credit.getNames(GcdStoryCredit.CreditType.PENCILS));
                        doc.setStoryPencilsCreatorId(credit.getIds(GcdStoryCredit.CreditType.PENCILS));
                        doc.setStoryInks(credit.getNames(GcdStoryCredit.CreditType.INKS));
                        doc.setStoryInksCreatorId(credit.getIds(GcdStoryCredit.CreditType.INKS));
                        doc.setStoryColors(credit.getNames(GcdStoryCredit.CreditType.COLORS));
                        doc.setStoryColorsCreatorId(credit.getIds(GcdStoryCredit.CreditType.COLORS));
                        doc.setStoryLetters(credit.getNames(GcdStoryCredit.CreditType.LETTERS));
                        doc.setStoryLettersCreatorId(credit.getIds(GcdStoryCredit.CreditType.LETTERS));
                        doc.setStoryEditing(credit.getNames(GcdStoryCredit.CreditType.STORY_EDITING));
                        doc.setStoryEditingCreatorId(credit.getIds(GcdStoryCredit.CreditType.STORY_EDITING));
                        doc.setStoryPainting(credit.getNames(GcdStoryCredit.CreditType.PAINTING));
                        doc.setStoryPaintingCreatorId(credit.getIds(GcdStoryCredit.CreditType.PAINTING));

                        doc.setStoryCreditSource("gcd_story_credit");
                    } else {
                        addOptionalMultiString(rs, "story_script", scriptors -> doc.setStoryScript(scriptors));
                        addOptionalMultiString(rs, "story_pencils", artists -> doc.setStoryPencils(artists));
                        addOptionalMultiString(rs, "story_inks", inkers -> doc.setStoryInks(inkers));
                        addOptionalMultiString(rs, "story_colors", colorists -> doc.setStoryColors(colorists));
                        addOptionalMultiString(rs, "story_letters", letterers -> doc.setStoryLetters(letterers));
                        addOptionalMultiString(rs, "story_editing", editors -> doc.setStoryEditing(editors));

                        doc.setStoryCreditSource("gcd_story");
                    }
                    addOptionalMultiString(rs, "story_genre", genres -> doc.setStoryGenre(genres));
                    addOptionalMultiString(rs, "story_characters", characters -> doc.setStoryCharacters(characters));
                    addOptionalStringFromId(rs, "strtypeid", metadata.getStoryTypeMap(), type -> doc.setStoryType(type));
                    addOptionalString(rs, "story_job_number", num -> doc.setStoryJobNumber(num));
                    if (schema.isStoryFirstLine()) {
                        addOptionalString(rs, "story_first_line", line -> doc.setStoryFirstLine(line));
                    }
                    addOptionalDateFromTimestamp(rs, "story_created", created -> doc.setStoryCreated(created));
                    addOptionalDateFromTimestamp(rs, "story_modified", modified -> doc.setStoryModified(modified));
                }
                writer.write(doc);
                if (++count % 2000000 == 0) {
                    log.info("Processed document #" + count);
                    writer.close();
                    writer = getParquetWriter(indexName, snapshot, ++part);
                }
            } catch (SQLException e) {
                log.warn("Skipping document due to: " + e.getMessage());
            }
        }
        writer.close();
        rs.close();
        st.close();
        conn.close();
        return count;
    }

    private static void addOptionalCredit(GcdStoryCredit credit,
            BiConsumer<String, List<CharSequence>> nameConsumer, BiConsumer<String, List<Long>> idConsumer) {
        for (GcdStoryCredit.CreditType type : GcdStoryCredit.CreditType.values()) {
            if (type.getField() != null) {
                final List<CharSequence> creditNames = credit.getNames(type);
                if (!creditNames.isEmpty()) {
                    nameConsumer.accept(type.getField(), creditNames);
                    final List<Long> creditIds = credit.getIds(type);
                    idConsumer.accept(type.getField() + "_creator_id", creditIds);
                }
            }
        }
    }

    private static String[] addOptionalMultiString(ResultSet rs, String field, Consumer<List<CharSequence>> consumer) throws SQLException {
        try {
            final String value = rs.getString(field);
            if (value != null) {
                String[] values = value.split("\\s*;\\s*");
                consumer.accept(Arrays.asList(values));
                return values;
            }
        } catch (SQLException e) {
            log.warn(e.getMessage());
        }
        return new String[0];
    }

    private static void addOptionalStringFromId(ResultSet rs, String rawField, Map<Integer, String> lookupMap, Consumer<String> consumer) {
        try {
            final int id = rs.getInt(rawField);
            if (lookupMap.containsKey(id)) {
                consumer.accept(lookupMap.get(id));
            }
        } catch (SQLException e) {
            log.warn(e.getMessage());
        }
    }

    private static void addOptionalString(final ResultSet rs, final String field, final Consumer<String> consumer) throws SQLException {
        try {
            final String value = rs.getString(field);
            if (value != null) {
                consumer.accept(value);
            }
        } catch (SQLException e) {
            log.warn(e.getMessage());
        }
    }

    private static void addLong(final ResultSet rs, final String field, final LongConsumer consumer) throws SQLException {
        consumer.accept(rs.getLong(field));
    }

    private static void addLongWithDefault(final ResultSet rs, final String field, final long defaultValue, final LongConsumer consumer) {
        try {
            final Long value = rs.getLong(field);
            if (value == null) {
                consumer.accept(defaultValue);
            } else {
                consumer.accept(value);
            }
        } catch (SQLException e) {
            log.warn(e.getMessage());
            consumer.accept(defaultValue);
        }
    }

    private static void addIntWithDefault(final ResultSet rs, final String field, final int defaultValue, final LongConsumer consumer) {
        try {
            final Integer value = rs.getInt(field);
            if (value == null) {
                consumer.accept(defaultValue);
            } else {
                consumer.accept(value);
            }
        } catch (SQLException e) {
            log.warn(e.getMessage());
            consumer.accept(defaultValue);
        }
    }

    private static void addOptionalInt(final ResultSet rs, final String field, final IntConsumer consumer) {
        try {
            consumer.accept(rs.getInt(field));
        } catch (SQLException e) {
            log.warn(e.getMessage());
        }
    }

    private static void addOptionalLong(final ResultSet rs, final String field, final LongConsumer consumer) {
        try {
            consumer.accept(rs.getLong(field));
        } catch (SQLException e) {
            log.warn(e.getMessage());
        }
    }

    private static void addOptionalDate(final ResultSet rs, final String rawField, final IntConsumer consumer) {
        try {
            final String dateString = rs.getString(rawField);
            final Matcher m = DATE_PATTERN.matcher(dateString);
            if (m.matches()) {
                consumer.accept(Integer.parseInt(String.format("%s%s%s", m.group(1), m.group(2), m.group(3))));
            } else {
                consumer.accept(-1);
            }
        } catch (SQLException e) {
            log.warn(e.getMessage());
            consumer.accept(-1);
        } catch (NumberFormatException e) {
            log.warn(e.getMessage());
            consumer.accept(-1);
        }
    }

    private static void addOptionalDateFromTimestamp(final ResultSet rs, final String field, final IntConsumer consumer) {
        try {
            final long unixTime = rs.getInt(field);
            if (unixTime > 0) {
                final Date time = new Date(unixTime * 1000L);
                consumer.accept(Integer.parseInt(COMPARABLE_DATE_FORMAT.format(time)));
            } else {
                consumer.accept(-1);
            }
        } catch (SQLException e) {
            log.warn(e.getMessage());
            consumer.accept(-1);
        }
    }
}
