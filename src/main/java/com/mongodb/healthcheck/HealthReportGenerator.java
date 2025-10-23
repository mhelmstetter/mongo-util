package com.mongodb.healthcheck;

import com.mongodb.healthcheck.ClusterIndexStats.CollectionStats;
import com.mongodb.healthcheck.ClusterIndexStats.DatabaseStats;
import com.mongodb.healthcheck.ClusterIndexStats.IndexInfo;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Generates HTML health check reports for MongoDB index usage
 */
public class HealthReportGenerator {

    private static final String MONGODB_GREEN = "#00684A";
    private static final String MONGODB_DARK = "#001E2B";
    private static final String MONGODB_YELLOW = "#E9FF99";
    private static final String WARNING_COLOR = "#FFC857";
    private static final String ERROR_COLOR = "#E63946";

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public String generateReport(ClusterIndexStats stats) {
        StringBuilder html = new StringBuilder();

        html.append("<!DOCTYPE html>\n");
        html.append("<html lang=\"en\">\n");
        html.append("<head>\n");
        html.append("    <meta charset=\"UTF-8\">\n");
        html.append("    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n");
        html.append("    <title>MongoDB Health Check Report</title>\n");
        appendStyles(html);
        html.append("</head>\n");
        html.append("<body>\n");

        // Header
        appendHeader(html);

        // Summary section
        appendSummary(html, stats);

        // Unused indexes section
        if (stats.getUnusedIndexCount() > 0) {
            appendUnusedIndexes(html, stats);
        }

        // Low usage indexes section
        if (stats.getLowUsageIndexCount() > 0) {
            appendLowUsageIndexes(html, stats);
        }

        // Database details
        appendDatabaseDetails(html, stats);

        // Footer
        appendFooter(html);

        // Scripts
        appendScripts(html);

        html.append("</body>\n");
        html.append("</html>\n");

        return html.toString();
    }

    private void appendStyles(StringBuilder html) {
        html.append("    <style>\n");
        html.append("        :root {\n");
        html.append("            --primary-color: #00684a;\n");
        html.append("            --primary-dark: #004d37;\n");
        html.append("            --secondary-color: #00ed64;\n");
        html.append("            --background-color: #0f172a;\n");
        html.append("            --card-background: rgba(255, 255, 255, 0.05);\n");
        html.append("            --text-primary: #e2e8f0;\n");
        html.append("            --text-secondary: #94a3b8;\n");
        html.append("            --border-color: rgba(255, 255, 255, 0.1);\n");
        html.append("        }\n");
        html.append("        * { margin: 0; padding: 0; box-sizing: border-box; }\n");
        html.append("        body {\n");
        html.append("            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;\n");
        html.append("            background-color: var(--background-color);\n");
        html.append("            color: var(--text-primary);\n");
        html.append("            min-height: 100vh;\n");
        html.append("        }\n");
        html.append("        .header {\n");
        html.append("            background-color: ").append(MONGODB_DARK).append(";\n");
        html.append("            color: white;\n");
        html.append("            padding: 20px;\n");
        html.append("            position: sticky;\n");
        html.append("            top: 0;\n");
        html.append("            z-index: 1000;\n");
        html.append("            box-shadow: 0 2px 4px rgba(0,0,0,0.1);\n");
        html.append("        }\n");
        html.append("        .header h1 {\n");
        html.append("            font-size: 24px;\n");
        html.append("            margin-bottom: 5px;\n");
        html.append("        }\n");
        html.append("        .header .subtitle {\n");
        html.append("            font-size: 14px;\n");
        html.append("            opacity: 0.8;\n");
        html.append("        }\n");
        html.append("        .container {\n");
        html.append("            max-width: 95%;\n");
        html.append("            margin: 0 auto;\n");
        html.append("            padding: 20px;\n");
        html.append("        }\n");
        html.append("        .summary {\n");
        html.append("            display: grid;\n");
        html.append("            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));\n");
        html.append("            gap: 15px;\n");
        html.append("            margin-bottom: 30px;\n");
        html.append("        }\n");
        html.append("        .summary-box {\n");
        html.append("            background: var(--card-background);\n");
        html.append("            padding: 20px;\n");
        html.append("            border-radius: 12px;\n");
        html.append("            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);\n");
        html.append("            border: 1px solid var(--border-color);\n");
        html.append("            border-left: 4px solid var(--primary-color);\n");
        html.append("            transition: transform 0.2s ease;\n");
        html.append("        }\n");
        html.append("        .summary-box:hover {\n");
        html.append("            transform: translateY(-2px);\n");
        html.append("        }\n");
        html.append("        .summary-box.warning {\n");
        html.append("            border-left-color: ").append(WARNING_COLOR).append(";\n");
        html.append("        }\n");
        html.append("        .summary-box.error {\n");
        html.append("            border-left-color: ").append(ERROR_COLOR).append(";\n");
        html.append("        }\n");
        html.append("        .summary-box h3 {\n");
        html.append("            font-size: 0.875rem;\n");
        html.append("            font-weight: 600;\n");
        html.append("            color: var(--text-secondary);\n");
        html.append("            text-transform: uppercase;\n");
        html.append("            letter-spacing: 0.5px;\n");
        html.append("            margin-bottom: 10px;\n");
        html.append("        }\n");
        html.append("        .summary-box .value {\n");
        html.append("            font-size: 2rem;\n");
        html.append("            font-weight: 700;\n");
        html.append("            color: var(--text-primary);\n");
        html.append("        }\n");
        html.append("        .section {\n");
        html.append("            background: var(--card-background);\n");
        html.append("            margin-bottom: 20px;\n");
        html.append("            border-radius: 12px;\n");
        html.append("            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);\n");
        html.append("            border: 1px solid var(--border-color);\n");
        html.append("            overflow: hidden;\n");
        html.append("        }\n");
        html.append("        .section-header {\n");
        html.append("            background: linear-gradient(135deg, var(--primary-color) 0%, var(--primary-dark) 100%);\n");
        html.append("            color: white;\n");
        html.append("            padding: 15px 20px;\n");
        html.append("            font-size: 1.125rem;\n");
        html.append("            font-weight: 600;\n");
        html.append("        }\n");
        html.append("        .section-content {\n");
        html.append("            padding: 20px;\n");
        html.append("        }\n");
        html.append("        table {\n");
        html.append("            width: 100%;\n");
        html.append("            border-collapse: collapse;\n");
        html.append("            font-size: 0.875rem;\n");
        html.append("            background: transparent;\n");
        html.append("            border-radius: 8px;\n");
        html.append("            overflow: hidden;\n");
        html.append("        }\n");
        html.append("        th, td {\n");
        html.append("            padding: 0.75rem;\n");
        html.append("            text-align: left;\n");
        html.append("            border-bottom: 1px solid var(--border-color);\n");
        html.append("        }\n");
        html.append("        th {\n");
        html.append("            background: linear-gradient(135deg, var(--primary-color) 0%, var(--primary-dark) 100%);\n");
        html.append("            color: white;\n");
        html.append("            font-weight: 600;\n");
        html.append("            cursor: pointer;\n");
        html.append("            user-select: none;\n");
        html.append("            position: relative;\n");
        html.append("            transition: background 0.2s ease;\n");
        html.append("            white-space: nowrap;\n");
        html.append("            font-size: 0.8125rem;\n");
        html.append("        }\n");
        html.append("        th:hover {\n");
        html.append("            background: linear-gradient(135deg, var(--primary-dark) 0%, #003d2e 100%);\n");
        html.append("        }\n");
        html.append("        td {\n");
        html.append("            color: var(--text-primary);\n");
        html.append("        }\n");
        html.append("        tr:nth-child(even) td {\n");
        html.append("            background-color: rgba(255, 255, 255, 0.02);\n");
        html.append("        }\n");
        html.append("        tr:hover td {\n");
        html.append("            background-color: rgba(255, 255, 255, 0.08) !important;\n");
        html.append("        }\n");
        html.append("        .number {\n");
        html.append("            text-align: right;\n");
        html.append("            font-family: 'Monaco', 'Menlo', 'Courier New', monospace;\n");
        html.append("            white-space: nowrap;\n");
        html.append("        }\n");
        html.append("        .col-namespace {\n");
        html.append("            max-width: 250px;\n");
        html.append("            overflow: hidden;\n");
        html.append("            text-overflow: ellipsis;\n");
        html.append("            white-space: nowrap;\n");
        html.append("        }\n");
        html.append("        .col-index-name {\n");
        html.append("            max-width: 200px;\n");
        html.append("            overflow: hidden;\n");
        html.append("            text-overflow: ellipsis;\n");
        html.append("            white-space: nowrap;\n");
        html.append("        }\n");
        html.append("        .col-key-pattern {\n");
        html.append("            max-width: 350px;\n");
        html.append("            white-space: normal;\n");
        html.append("            word-break: break-all;\n");
        html.append("        }\n");
        html.append("        .col-accesses, .col-size, .col-since {\n");
        html.append("            width: 90px;\n");
        html.append("        }\n");
        html.append("        code {\n");
        html.append("            background-color: rgba(0, 0, 0, 0.3);\n");
        html.append("            color: var(--secondary-color);\n");
        html.append("            padding: 2px 6px;\n");
        html.append("            border-radius: 3px;\n");
        html.append("            font-size: 0.8125rem;\n");
        html.append("            font-family: 'Monaco', 'Menlo', 'Courier New', monospace;\n");
        html.append("        }\n");
        html.append("        .badge {\n");
        html.append("            display: inline-block;\n");
        html.append("            padding: 3px 8px;\n");
        html.append("            border-radius: 4px;\n");
        html.append("            font-size: 0.6875rem;\n");
        html.append("            font-weight: 600;\n");
        html.append("            text-transform: uppercase;\n");
        html.append("            letter-spacing: 0.5px;\n");
        html.append("            margin-left: 6px;\n");
        html.append("        }\n");
        html.append("        .badge.shard-key {\n");
        html.append("            background-color: ").append(MONGODB_YELLOW).append(";\n");
        html.append("            color: ").append(MONGODB_DARK).append(";\n");
        html.append("        }\n");
        html.append("        .badge.unused {\n");
        html.append("            background-color: ").append(ERROR_COLOR).append(";\n");
        html.append("            color: white;\n");
        html.append("        }\n");
        html.append("        .badge.low-usage {\n");
        html.append("            background-color: ").append(WARNING_COLOR).append(";\n");
        html.append("            color: ").append(MONGODB_DARK).append(";\n");
        html.append("        }\n");
        html.append("        .filter-box {\n");
        html.append("            margin-bottom: 15px;\n");
        html.append("        }\n");
        html.append("        .filter-box input {\n");
        html.append("            width: 100%;\n");
        html.append("            padding: 0.75rem;\n");
        html.append("            border: 1px solid var(--border-color);\n");
        html.append("            border-radius: 8px;\n");
        html.append("            font-size: 0.875rem;\n");
        html.append("            background-color: rgba(255, 255, 255, 0.05);\n");
        html.append("            color: var(--text-primary);\n");
        html.append("            transition: border-color 0.2s ease;\n");
        html.append("        }\n");
        html.append("        .filter-box input:focus {\n");
        html.append("            outline: none;\n");
        html.append("            border-color: var(--primary-color);\n");
        html.append("            box-shadow: 0 0 0 3px rgba(0, 104, 74, 0.1);\n");
        html.append("        }\n");
        html.append("        .filter-box input::placeholder {\n");
        html.append("            color: var(--text-secondary);\n");
        html.append("        }\n");
        html.append("        .footer {\n");
        html.append("            text-align: center;\n");
        html.append("            padding: 20px;\n");
        html.append("            color: var(--text-secondary);\n");
        html.append("            font-size: 0.75rem;\n");
        html.append("        }\n");
        html.append("        .database-name {\n");
        html.append("            font-weight: 600;\n");
        html.append("            color: var(--primary-color);\n");
        html.append("        }\n");
        html.append("        a {\n");
        html.append("            color: var(--secondary-color);\n");
        html.append("            text-decoration: none;\n");
        html.append("            transition: color 0.2s ease;\n");
        html.append("        }\n");
        html.append("        a:hover {\n");
        html.append("            color: #00ff75;\n");
        html.append("            text-decoration: underline;\n");
        html.append("        }\n");
        html.append("    </style>\n");
    }

    private void appendHeader(StringBuilder html) {
        html.append("    <div class=\"header\">\n");
        html.append("        <h1>MongoDB Health Check Report</h1>\n");
        html.append("        <div class=\"subtitle\">Generated: ").append(dateFormat.format(new Date())).append("</div>\n");
        html.append("    </div>\n");
    }

    private void appendSummary(StringBuilder html, ClusterIndexStats stats) {
        html.append("    <div class=\"container\">\n");
        html.append("        <div class=\"summary\">\n");

        // Total indexes
        html.append("            <div class=\"summary-box\">\n");
        html.append("                <h3>Total Indexes</h3>\n");
        html.append("                <div class=\"value\">").append(stats.getTotalIndexCount()).append("</div>\n");
        html.append("            </div>\n");

        // Unused indexes
        String unusedClass = stats.getUnusedIndexCount() > 0 ? "error" : "";
        html.append("            <div class=\"summary-box ").append(unusedClass).append("\">\n");
        html.append("                <h3>Unused Indexes</h3>\n");
        html.append("                <div class=\"value\">").append(stats.getUnusedIndexCount()).append("</div>\n");
        html.append("            </div>\n");

        // Low usage indexes
        String lowUsageClass = stats.getLowUsageIndexCount() > 0 ? "warning" : "";
        html.append("            <div class=\"summary-box ").append(lowUsageClass).append("\">\n");
        html.append("                <h3>Low Usage Indexes</h3>\n");
        html.append("                <div class=\"value\">").append(stats.getLowUsageIndexCount()).append("</div>\n");
        html.append("            </div>\n");

        // Databases
        html.append("            <div class=\"summary-box\">\n");
        html.append("                <h3>Databases</h3>\n");
        html.append("                <div class=\"value\">").append(stats.getDatabases().size()).append("</div>\n");
        html.append("            </div>\n");

        html.append("        </div>\n");
    }

    private void appendUnusedIndexes(StringBuilder html, ClusterIndexStats stats) {
        List<IndexInfo> unusedIndexes = stats.getUnusedIndexes();

        html.append("        <div class=\"section\">\n");
        html.append("            <div class=\"section-header\">Unused Indexes (").append(unusedIndexes.size()).append(")</div>\n");
        html.append("            <div class=\"section-content\">\n");
        html.append("                <div class=\"filter-box\">\n");
        html.append("                    <input type=\"text\" id=\"unusedFilter\" placeholder=\"Filter by namespace or index name...\" onkeyup=\"filterTable('unusedTable', 'unusedFilter')\">\n");
        html.append("                </div>\n");
        html.append("                <table id=\"unusedTable\" class=\"sortable\">\n");
        html.append("                    <thead>\n");
        html.append("                        <tr>\n");
        html.append("                            <th onclick=\"sortTable('unusedTable', 0)\">Namespace ▼</th>\n");
        html.append("                            <th onclick=\"sortTable('unusedTable', 1)\">Key Pattern</th>\n");
        html.append("                            <th onclick=\"sortTable('unusedTable', 2)\" class=\"number\">Total Accesses</th>\n");
        html.append("                            <th onclick=\"sortTable('unusedTable', 3)\" class=\"number\">Primary</th>\n");
        html.append("                            <th onclick=\"sortTable('unusedTable', 4)\" class=\"number\">Secondary</th>\n");
        html.append("                            <th onclick=\"sortTable('unusedTable', 5)\" class=\"number\">Size (MB)</th>\n");
        html.append("                            <th onclick=\"sortTable('unusedTable', 6)\">Since</th>\n");
        html.append("                        </tr>\n");
        html.append("                    </thead>\n");
        html.append("                    <tbody>\n");

        for (IndexInfo idx : unusedIndexes) {
            html.append("                        <tr>\n");
            html.append("                            <td class=\"col-namespace\"><a href=\"#collection-").append(escapeHtml(idx.getNamespace())).append("\">").append(escapeHtml(idx.getNamespace())).append("</a></td>\n");
            html.append("                            <td class=\"col-key-pattern\"><code>").append(escapeHtml(idx.getKeyPattern())).append("</code><span class=\"badge unused\">Unused</span></td>\n");
            html.append("                            <td class=\"number col-accesses\">").append(idx.getTotalAccesses()).append("</td>\n");
            html.append("                            <td class=\"number col-accesses\">").append(idx.getPrimaryAccesses()).append("</td>\n");
            html.append("                            <td class=\"number col-accesses\">").append(idx.getSecondaryAccesses()).append("</td>\n");
            html.append("                            <td class=\"number col-size\">").append(String.format("%.2f", idx.getSizeMB())).append("</td>\n");
            html.append("                            <td class=\"col-since\">").append(idx.getSince() != null ? dateFormat.format(idx.getSince()) : "N/A").append("</td>\n");
            html.append("                        </tr>\n");
        }

        html.append("                    </tbody>\n");
        html.append("                </table>\n");
        html.append("            </div>\n");
        html.append("        </div>\n");
    }

    private void appendLowUsageIndexes(StringBuilder html, ClusterIndexStats stats) {
        List<IndexInfo> lowUsageIndexes = stats.getLowUsageIndexes();

        html.append("        <div class=\"section\">\n");
        html.append("            <div class=\"section-header\">Low Usage Indexes (").append(lowUsageIndexes.size()).append(")</div>\n");
        html.append("            <div class=\"section-content\">\n");
        html.append("                <div class=\"filter-box\">\n");
        html.append("                    <input type=\"text\" id=\"lowUsageFilter\" placeholder=\"Filter by namespace or index name...\" onkeyup=\"filterTable('lowUsageTable', 'lowUsageFilter')\">\n");
        html.append("                </div>\n");
        html.append("                <table id=\"lowUsageTable\" class=\"sortable\">\n");
        html.append("                    <thead>\n");
        html.append("                        <tr>\n");
        html.append("                            <th onclick=\"sortTable('lowUsageTable', 0)\">Namespace ▼</th>\n");
        html.append("                            <th onclick=\"sortTable('lowUsageTable', 1)\">Key Pattern</th>\n");
        html.append("                            <th onclick=\"sortTable('lowUsageTable', 2)\" class=\"number\">Total Accesses</th>\n");
        html.append("                            <th onclick=\"sortTable('lowUsageTable', 3)\" class=\"number\">Primary</th>\n");
        html.append("                            <th onclick=\"sortTable('lowUsageTable', 4)\" class=\"number\">Secondary</th>\n");
        html.append("                            <th onclick=\"sortTable('lowUsageTable', 5)\" class=\"number\">Size (MB)</th>\n");
        html.append("                            <th onclick=\"sortTable('lowUsageTable', 6)\">Since</th>\n");
        html.append("                        </tr>\n");
        html.append("                    </thead>\n");
        html.append("                    <tbody>\n");

        for (IndexInfo idx : lowUsageIndexes) {
            html.append("                        <tr>\n");
            html.append("                            <td class=\"col-namespace\"><a href=\"#collection-").append(escapeHtml(idx.getNamespace())).append("\">").append(escapeHtml(idx.getNamespace())).append("</a></td>\n");
            html.append("                            <td class=\"col-key-pattern\"><code>").append(escapeHtml(idx.getKeyPattern())).append("</code><span class=\"badge low-usage\">Low Usage</span></td>\n");
            html.append("                            <td class=\"number col-accesses\">").append(idx.getTotalAccesses()).append("</td>\n");
            html.append("                            <td class=\"number col-accesses\">").append(idx.getPrimaryAccesses()).append("</td>\n");
            html.append("                            <td class=\"number col-accesses\">").append(idx.getSecondaryAccesses()).append("</td>\n");
            html.append("                            <td class=\"number col-size\">").append(String.format("%.2f", idx.getSizeMB())).append("</td>\n");
            html.append("                            <td class=\"col-since\">").append(idx.getSince() != null ? dateFormat.format(idx.getSince()) : "N/A").append("</td>\n");
            html.append("                        </tr>\n");
        }

        html.append("                    </tbody>\n");
        html.append("                </table>\n");
        html.append("            </div>\n");
        html.append("        </div>\n");
    }

    private void appendDatabaseDetails(StringBuilder html, ClusterIndexStats stats) {
        for (DatabaseStats db : stats.getDatabases()) {
            html.append("        <div class=\"section\">\n");
            html.append("            <div class=\"section-header\">Database: ").append(escapeHtml(db.getName())).append("</div>\n");
            html.append("            <div class=\"section-content\">\n");

            for (CollectionStats coll : db.getCollections()) {
                html.append("                <h3 class=\"database-name\" id=\"collection-").append(escapeHtml(coll.getNamespace())).append("\">").append(escapeHtml(coll.getNamespace()));
                if (coll.getShardKey() != null) {
                    html.append(" <span class=\"badge shard-key\">Sharded</span>");
                }
                html.append("</h3>\n");

                if (coll.getShardKey() != null) {
                    html.append("                <p><strong>Shard Key:</strong> <code>").append(escapeHtml(coll.getShardKey())).append("</code></p>\n");
                }

                html.append("                <table class=\"sortable\">\n");
                html.append("                    <thead>\n");
                html.append("                        <tr>\n");
                html.append("                            <th>Key Pattern ▼</th>\n");
                html.append("                            <th class=\"number\">Total Accesses</th>\n");
                html.append("                            <th class=\"number\">Primary</th>\n");
                html.append("                            <th class=\"number\">Secondary</th>\n");
                html.append("                            <th class=\"number\">Size (MB)</th>\n");
                html.append("                            <th>Since</th>\n");
                html.append("                        </tr>\n");
                html.append("                    </thead>\n");
                html.append("                    <tbody>\n");

                for (IndexInfo idx : coll.getIndexes()) {
                    html.append("                        <tr>\n");
                    html.append("                            <td class=\"col-key-pattern\"><code>").append(escapeHtml(idx.getKeyPattern())).append("</code>");
                    if (idx.isShardKey()) {
                        html.append("<span class=\"badge shard-key\">Shard Key</span>");
                    }
                    if (idx.isUnused() && !idx.isShardKey()) {
                        html.append("<span class=\"badge unused\">Unused</span>");
                    } else if (idx.isLowUsage() && !idx.isShardKey()) {
                        html.append("<span class=\"badge low-usage\">Low Usage</span>");
                    }
                    html.append("</td>\n");
                    html.append("                            <td class=\"number col-accesses\">").append(idx.getTotalAccesses()).append("</td>\n");
                    html.append("                            <td class=\"number col-accesses\">").append(idx.getPrimaryAccesses()).append("</td>\n");
                    html.append("                            <td class=\"number col-accesses\">").append(idx.getSecondaryAccesses()).append("</td>\n");
                    html.append("                            <td class=\"number col-size\">").append(String.format("%.2f", idx.getSizeMB())).append("</td>\n");
                    html.append("                            <td class=\"col-since\">").append(idx.getSince() != null ? dateFormat.format(idx.getSince()) : "N/A").append("</td>\n");
                    html.append("                        </tr>\n");
                }

                html.append("                    </tbody>\n");
                html.append("                </table>\n");
                html.append("                <br>\n");
            }

            html.append("            </div>\n");
            html.append("        </div>\n");
        }

        html.append("    </div>\n");
    }

    private void appendFooter(StringBuilder html) {
        html.append("    <div class=\"footer\">\n");
        html.append("        <p>MongoDB Health Check Report - Generated by mongo-util</p>\n");
        html.append("    </div>\n");
    }

    private void appendScripts(StringBuilder html) {
        html.append("    <script>\n");
        html.append("        function sortTable(tableId, column) {\n");
        html.append("            var table = document.getElementById(tableId);\n");
        html.append("            var rows = Array.from(table.querySelectorAll('tbody tr'));\n");
        html.append("            var ascending = table.dataset.sortColumn == column && table.dataset.sortOrder == 'asc';\n");
        html.append("            \n");
        html.append("            rows.sort(function(a, b) {\n");
        html.append("                var aVal = a.cells[column].textContent.trim();\n");
        html.append("                var bVal = b.cells[column].textContent.trim();\n");
        html.append("                \n");
        html.append("                var aNum = parseFloat(aVal);\n");
        html.append("                var bNum = parseFloat(bVal);\n");
        html.append("                \n");
        html.append("                if (!isNaN(aNum) && !isNaN(bNum)) {\n");
        html.append("                    return ascending ? aNum - bNum : bNum - aNum;\n");
        html.append("                }\n");
        html.append("                \n");
        html.append("                return ascending ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);\n");
        html.append("            });\n");
        html.append("            \n");
        html.append("            var tbody = table.querySelector('tbody');\n");
        html.append("            tbody.innerHTML = '';\n");
        html.append("            rows.forEach(function(row) { tbody.appendChild(row); });\n");
        html.append("            \n");
        html.append("            table.dataset.sortColumn = column;\n");
        html.append("            table.dataset.sortOrder = ascending ? 'desc' : 'asc';\n");
        html.append("        }\n");
        html.append("        \n");
        html.append("        function filterTable(tableId, filterId) {\n");
        html.append("            var input = document.getElementById(filterId);\n");
        html.append("            var filter = input.value.toLowerCase();\n");
        html.append("            var table = document.getElementById(tableId);\n");
        html.append("            var rows = table.querySelectorAll('tbody tr');\n");
        html.append("            \n");
        html.append("            rows.forEach(function(row) {\n");
        html.append("                var text = row.textContent.toLowerCase();\n");
        html.append("                row.style.display = text.includes(filter) ? '' : 'none';\n");
        html.append("            });\n");
        html.append("        }\n");
        html.append("    </script>\n");
    }

    private String escapeHtml(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                  .replace("<", "&lt;")
                  .replace(">", "&gt;")
                  .replace("\"", "&quot;")
                  .replace("'", "&#39;");
    }
}
