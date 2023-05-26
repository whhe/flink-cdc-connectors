/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.oceanbase.source;

import org.apache.commons.lang3.StringUtils;

import java.util.List;

/** OceanBase dialect for mysql mode. */
public class OceanBaseMysqlDialect implements OceanBaseDialect {

    @Override
    public String getFullTableName(String db, String table) {
        return String.format("`%s`.`%s`", db, table);
    }

    @Override
    public String getValidationSql() {
        return "SELECT 1";
    }

    @Override
    public String getQueryPkColumnSql(String dbName, String tableName) {
        return String.format(
                "SELECT COLUMN_NAME FROM information_schema.statistics WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND upper(INDEX_NAME) = 'PRIMARY'",
                dbName, tableName);
    }

    @Override
    public String getQueryNewChunkBoundSql(
            String fullTableName,
            List<String> indexNames,
            List<Object> oldBound,
            Integer chunkSize) {
        String whereClause;
        String limitClause;
        if (oldBound == null) {
            limitClause = "LIMIT 1";
            whereClause = "";
        } else {
            limitClause = String.format("LIMIT %d,1", chunkSize - 1);
            whereClause = "WHERE " + getConditionGreat(indexNames, oldBound);
        }
        String selectFields = StringUtils.join(indexNames, ",");
        return String.format(
                "SELECT %s FROM %s %s ORDER BY %s ASC %s",
                selectFields, fullTableName, whereClause, selectFields, limitClause);
    }

    @Override
    public String getQueryChunkSql(
            String fullTableName, List<String> indexNames, List<Object> left, List<Object> right) {
        String whereClause;
        if (left == null) {
            if (right == null) {
                whereClause = "";
            } else {
                whereClause = "WHERE " + getConditionLessOrEqual(indexNames, right);
            }
        } else {
            if (right == null) {
                whereClause = "WHERE " + getConditionGreat(indexNames, left);
            } else {
                whereClause =
                        String.format(
                                "WHERE (%s) AND (%s)",
                                getConditionGreat(indexNames, left),
                                getConditionLessOrEqual(indexNames, right));
            }
        }
        return String.format("SELECT * FROM %s %s", fullTableName, whereClause);
    }
}
