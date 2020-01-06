package org.gcd.etl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author jack@indeed.com (Jack Humphrey)
 */
public class GcdStoryCredit {

    private final long storyId;

    // TODO: refactor to builder pattern and lower-overhead data types as data size grows
    private final Map<CreditType, List<CharSequence>> creatorNamesByType;
    private final Map<CreditType, List<Long>> creatorIdsByType;

    public enum CreditType {
        SCRIPT(1, "story_script"),
        PENCILS(2, "story_pencils"),
        INKS(3, "story_inks"),
        COLORS(4, "story_colors"),
        LETTERS(5, "story_letters"),
        STORY_EDITING(6, "story_editing"),
        PENCILS_INKS(7, new CreditType[] { PENCILS, INKS }),
        PENCILS_INKS_COLORS(8, new CreditType[] { PENCILS, INKS, COLORS }),
        PAINTING(9, "story_painting"),
        SCRIPT_PENCILS_INKS(10, new CreditType[] { SCRIPT, PENCILS, INKS }),
        SCRIPT_PENCILS_INKS_COLORS(11, new CreditType[] { SCRIPT, PENCILS, INKS, COLORS }),
        SCRIPT_PENCILS_INKS_LETTERs(12, new CreditType[] { SCRIPT, PENCILS, INKS, LETTERS }),
        SCRIPT_PENCILS_INKS_COLORS_LETTERS(13, new CreditType[] { SCRIPT, PENCILS, INKS, COLORS, LETTERS });

        private final int id;
        private final String field;
        private final CreditType[] parts;

        CreditType(final int id, final String field) {
            this.id = id;
            this.field = field;
            this.parts = new CreditType[0];
        }

        CreditType(final int id, final CreditType[] parts) {
            this.id = id;
            this.field = null;
            this.parts = parts;
        }

        public String getField() {
            return field;
        }
    }

    private static final CreditType[] CREDIT_TYPES = new CreditType[] {
            null,
            CreditType.SCRIPT,
            CreditType.PENCILS,
            CreditType.INKS,
            CreditType.COLORS,
            CreditType.LETTERS,
            CreditType.STORY_EDITING,
            CreditType.PENCILS_INKS,
            CreditType.PENCILS_INKS_COLORS,
            CreditType.PAINTING,
            CreditType.SCRIPT_PENCILS_INKS,
            CreditType.SCRIPT_PENCILS_INKS_COLORS,
            CreditType.SCRIPT_PENCILS_INKS_LETTERs,
            CreditType.SCRIPT_PENCILS_INKS_COLORS_LETTERS
    };

    private GcdStoryCredit(final long storyId) {
        this.storyId = storyId;
        this.creatorNamesByType = new HashMap<>();
        this.creatorIdsByType = new HashMap<>();
    }

    private void addCredit(final int creditTypeId, final long creatorId, final String creatorName) {
        final CreditType type = CREDIT_TYPES[creditTypeId];
        if (type.parts.length > 0) {
            for (CreditType part : type.parts) {
                addCredit(part.id, creatorId, creatorName);
            }
        } else {
            final List<CharSequence> names = creatorNamesByType.computeIfAbsent(type, k -> new ArrayList<>());
            names.add(creatorName);
            final List<Long> ids = creatorIdsByType.computeIfAbsent(type, k -> new ArrayList<>());
            ids.add(creatorId);
        }
    }

    public List<CharSequence> getNames(final CreditType type) {
        if (type.parts.length > 0) {
            throw new IllegalArgumentException("Do not use multi-part types with this method");
        }
        return creatorNamesByType.getOrDefault(type, ImmutableList.of());
    }

    public List<Long> getIds(final CreditType type) {
        if (type.parts.length > 0) {
            throw new IllegalArgumentException("Do not use multi-part types with this method");
        }
        return creatorIdsByType.getOrDefault(type, ImmutableList.of());
    }

    public static Map<Long, GcdStoryCredit> loadAllStoryCredits(Connection conn) throws SQLException {
        final ImmutableMap.Builder<Long, GcdStoryCredit> builder = new ImmutableMap.Builder<>();
        final String query =
                "SELECT c.story_id AS story_id, c.credit_type_id AS credit_type_id, cr.gcd_official_name AS name, cr.id AS creator_id " +
                "FROM gcd_story_credit c INNER JOIN gcd_creator_name_detail n ON c.creator_id = n.id " +
                "     INNER JOIN gcd_creator cr ON n.creator_id = cr.id " +
                "ORDER BY c.story_id";
        final Statement st = conn.createStatement();
        final ResultSet rs = st.executeQuery(query);
        GcdStoryCredit cur = new GcdStoryCredit(0);
        while (rs.next()) {
            final long storyId = rs.getLong("story_id");
            if (cur.storyId != storyId) {
                cur = new GcdStoryCredit(storyId);
                builder.put(storyId, cur);
            }
            final int creditTypeId = rs.getInt("credit_type_id");
            final String name = rs.getString("name");
            final long creatorId = rs.getLong("creator_id");
            cur.addCredit(creditTypeId, creatorId, name);
        }
        rs.close();
        st.close();
        return builder.build();
    }
}
