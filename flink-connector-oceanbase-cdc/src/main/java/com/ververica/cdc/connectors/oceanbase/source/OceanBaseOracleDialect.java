/*
 * Copyright 2023 Ververica Inc.
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

import javax.annotation.Nonnull;

import java.util.List;

/** OceanBase dialect for Oracle mode. */
public class OceanBaseOracleDialect extends OceanBaseDialect {

    private static final long serialVersionUID = 1;

    @Override
    public String quoteIdentifier(@Nonnull String identifier) {
        return identifier;
    }

    @Override
    public String getQueryNewChunkBoundSql(
            @Nonnull String dbName,
            @Nonnull String tableName,
            List<String> chunkKeyColumns,
            List<Object> oldChunkBound,
            Integer chunkSize) {
        String fullTableName = getFullTableName(dbName, tableName);
        String whereClause;
        String limitClause;
        if (oldChunkBound == null) {
            limitClause = "WHERE RN=1";
            whereClause = "";
        } else {
            limitClause = String.format("WHERE RN=%d", chunkSize);
            whereClause = "WHERE " + getConditionGreat(chunkKeyColumns, oldChunkBound);
        }
        String selectFields = StringUtils.join(chunkKeyColumns, ",");
        return String.format(
                "SELECT %s FROM (SELECT %s, ROWNUM RN FROM (SELECT %s FROM %s ORDER BY %s ASC) %s) %s",
                selectFields,
                selectFields,
                selectFields,
                fullTableName,
                selectFields,
                whereClause,
                limitClause);
    }
}
