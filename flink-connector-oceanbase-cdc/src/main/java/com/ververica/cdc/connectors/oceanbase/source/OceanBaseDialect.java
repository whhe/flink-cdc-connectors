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

import java.util.List;

/** OceanBase dialect. */
public interface OceanBaseDialect {

    String getFullTableName(String db, String table);

    String getValidationSql();

    String getQueryPkColumnSql(String dbName, String tableName);

    String getQueryNewChunkBoundSql(
            String fullTableName,
            List<String> indexNames,
            List<Object> oldBound,
            Integer chunkSize);

    String getQueryChunkSql(
            String fullTableName, List<String> indexNames, List<Object> left, List<Object> right);

    default String getConditionLessOrEqual(List<String> fieldNames, List<Object> values) {
        StringBuilder cond = new StringBuilder();
        cond.append("((");
        for (int i = 0; i < fieldNames.size(); i++) {
            if (0 != i) {
                cond.append(" or (");
            }
            for (int j = 0; j < i; j++) {
                cond.append(fieldNames.get(j));
                cond.append(" = ");
                cond.append(values.get(j));
                cond.append(" and ");
            }
            cond.append(fieldNames.get(i));
            cond.append(i == fieldNames.size() - 1 ? " <= " : " < ");
            cond.append(values.get(i));
            cond.append(")");
        }
        return cond.append(")").toString();
    }

    default String getConditionGreat(List<String> fieldNames, List<Object> values) {
        StringBuilder cond = new StringBuilder();
        cond.append("((");
        for (int i = 0; i < fieldNames.size(); ++i) {
            if (0 != i) {
                cond.append(" or (");
            }
            for (int j = 0; j < i; ++j) {
                cond.append(fieldNames.get(j));
                cond.append(" = ");
                cond.append(values.get(j));
                cond.append(" and ");
            }
            cond.append(fieldNames.get(i));
            cond.append(" > ");
            cond.append(values.get(i));
            cond.append(")");
        }
        return cond.append(")").toString();
    }
}
