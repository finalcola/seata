/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.rm.datasource.undo;

import com.alibaba.fastjson.JSON;
import io.seata.common.util.StringUtils;
import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.model.Result;
import io.seata.rm.datasource.DataCompareUtils;
import io.seata.rm.datasource.sql.struct.Field;
import io.seata.rm.datasource.sql.struct.KeyType;
import io.seata.rm.datasource.sql.struct.Row;
import io.seata.rm.datasource.sql.struct.TableMeta;
import io.seata.rm.datasource.sql.struct.TableRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * The type Abstract undo executor.
 *
 * @author sharajava
 * @author Geng Zhang
 */
public abstract class AbstractUndoExecutor {

    /**
     * Logger for AbstractUndoExecutor
     **/
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractUndoExecutor.class);

    /**
     * template of check sql
     *
     * TODO support multiple primary key
     */
    private static final String CHECK_SQL_TEMPLATE = "SELECT * FROM %s WHERE %s in (%s)";

    /**
     * Switch of undo data validation
     */
    public static final boolean IS_UNDO_DATA_VALIDATION_ENABLE = ConfigurationFactory.getInstance()
            .getBoolean(ConfigurationKeys.TRANSACTION_UNDO_DATA_VALIDATION, true);

    /**
     * The Sql undo log.
     */
    protected SQLUndoLog sqlUndoLog;

    /**
     * Build undo sql string.
     *
     * @return the string
     */
    protected abstract String buildUndoSQL();

    /**
     * Instantiates a new Abstract undo executor.
     *
     * @param sqlUndoLog the sql undo log
     */
    public AbstractUndoExecutor(SQLUndoLog sqlUndoLog) {
        this.sqlUndoLog = sqlUndoLog;
    }

    /**
     * Gets sql undo log.
     *
     * @return the sql undo log
     */
    public SQLUndoLog getSqlUndoLog() {
        return sqlUndoLog;
    }

    /**
     * Execute on. 将undoLog解析为sql并执行，完成回滚
     *
     * @param conn the conn
     * @throws SQLException the sql exception
     */
    public void executeOn(Connection conn) throws SQLException {
        // 检查是否存在脏数据
        if (IS_UNDO_DATA_VALIDATION_ENABLE && !dataValidationAndGoOn(conn)) {
            return;
        }

        try {
            // 构建undo log的SQL
            String undoSQL = buildUndoSQL();
            // 编译sql
            PreparedStatement undoPST = conn.prepareStatement(undoSQL);

            TableRecords undoRows = getUndoRows();

            for (Row undoRow : undoRows.getRows()) {
                // 解析普通字段和主键
                ArrayList<Field> undoValues = new ArrayList<>();
                Field pkValue = null;
                for (Field field : undoRow.getFields()) {
                    if (field.getKeyType() == KeyType.PrimaryKey) {
                        pkValue = field;
                    } else {
                        undoValues.add(field);
                    }
                }
                // 设置prepareStatement参数
                undoPrepare(undoPST, undoValues, pkValue);
                // 执行更新
                undoPST.executeUpdate();
            }

        } catch (Exception ex) {
            if (ex instanceof SQLException) {
                throw (SQLException) ex;
            } else {
                throw new SQLException(ex);
            }

        }

    }

    /**
     * Undo prepare.
     *
     * @param undoPST    the undo pst
     * @param undoValues the undo values
     * @param pkValue    the pk value
     * @throws SQLException the sql exception
     */
    protected void undoPrepare(PreparedStatement undoPST, ArrayList<Field> undoValues, Field pkValue)
        throws SQLException {
        int undoIndex = 0;
        // 设置普通参数
        for (Field undoValue : undoValues) {
            undoIndex++;
            undoPST.setObject(undoIndex, undoValue.getValue(), undoValue.getType());
        }
        // PK is at last one.
        // INSERT INTO a (x, y, z, pk) VALUES (?, ?, ?, ?)
        // UPDATE a SET x=?, y=?, z=? WHERE pk = ?
        // DELETE FROM a WHERE pk = ?
        undoIndex++;
        // 设置where条件中的主键
        undoPST.setObject(undoIndex, pkValue.getValue(), pkValue.getType());
    }

    /**
     * Gets undo rows.
     *
     * @return the undo rows
     */
    protected abstract TableRecords getUndoRows();

    /**
     * Data validation.
     *
     * @param conn the conn
     * @return return true if data validation is ok and need continue undo, and return false if no need continue undo.
     * @throws SQLException the sql exception such as has dirty data
     */
    protected boolean dataValidationAndGoOn(Connection conn) throws SQLException {

        TableRecords beforeRecords = sqlUndoLog.getBeforeImage();
        TableRecords afterRecords = sqlUndoLog.getAfterImage();

        // 如果前后快照相同，则不需要进行undo
        // Compare current data with before data
        // No need undo if the before data snapshot is equivalent to the after data snapshot.
        Result<Boolean> beforeEqualsAfterResult = DataCompareUtils.isRecordsEquals(beforeRecords, afterRecords);
        if (beforeEqualsAfterResult.getResult()) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Stop rollback because there is no data change " +
                    "between the before data snapshot and the after data snapshot.");
            }
            // no need continue undo.
            return false;
        }

        // 检查数据是否是脏数据（查询当前最新的数据与更新后快照进行比较）
        // Validate if data is dirty.
        TableRecords currentRecords = queryCurrentRecords(conn);
        // compare with current data and after image.
        Result<Boolean> afterEqualsCurrentResult = DataCompareUtils.isRecordsEquals(afterRecords, currentRecords);
        // 如果发生了更新(更新后快照与当前最新数据不同)
        if (!afterEqualsCurrentResult.getResult()) {

            // If current data is not equivalent to the after data, then compare the current data with the before 
            // data, too. No need continue to undo if current data is equivalent to the before data snapshot
            // 比较更新前快照和最新数据，如果相等，则也不需要回滚
            Result<Boolean> beforeEqualsCurrentResult = DataCompareUtils.isRecordsEquals(beforeRecords, currentRecords);
            if (beforeEqualsCurrentResult.getResult()) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Stop rollback because there is no data change " +
                        "between the before data snapshot and the current data snapshot.");
                }
                // no need continue undo.
                return false;
            } else {
                if (LOGGER.isInfoEnabled()) {
                    if (StringUtils.isNotBlank(afterEqualsCurrentResult.getErrMsg())) {
                        LOGGER.info(afterEqualsCurrentResult.getErrMsg(), afterEqualsCurrentResult.getErrMsgParams());
                    }
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("check dirty datas failed, old and new data are not equal," +
                        "tableName:[" + sqlUndoLog.getTableName() + "]," +
                        "oldRows:[" + JSON.toJSONString(afterRecords.getRows()) + "]," +
                        "newRows:[" + JSON.toJSONString(currentRecords.getRows()) + "].");
                }
                // 存在脏数据，抛出异常
                throw new SQLException("Has dirty records when undo.");
            }
        }
        return true;
    }

    /**
     * Query current records.
     *
     * @param conn the conn
     * @return the table records
     * @throws SQLException the sql exception
     */
    protected TableRecords queryCurrentRecords(Connection conn) throws SQLException {
        TableRecords undoRecords = getUndoRows();
        TableMeta tableMeta = undoRecords.getTableMeta();
        String pkName = tableMeta.getPkName();
        int pkType = tableMeta.getColumnMeta(pkName).getDataType();

        // 解析主键值
        Object[] pkValues = parsePkValues(getUndoRows());
        if (pkValues.length == 0) {
            return TableRecords.empty(tableMeta);
        }
        StringBuffer replace = new StringBuffer();
        for (int i = 0; i < pkValues.length; i++) {
            replace.append("?,");
        }
        // build check sql
        String checkSQL = String.format(CHECK_SQL_TEMPLATE, sqlUndoLog.getTableName(), pkName,
            replace.substring(0, replace.length() - 1));

        PreparedStatement statement = null;
        ResultSet checkSet = null;
        TableRecords currentRecords;
        try {
            // 编译sql并设置参数
            statement = conn.prepareStatement(checkSQL);
            for (int i = 1; i <= pkValues.length; i++) {
                statement.setObject(i, pkValues[i - 1], pkType);
            }
            checkSet = statement.executeQuery();
            // 当前值
            currentRecords = TableRecords.buildRecords(tableMeta, checkSet);
        } finally {
            if (checkSet != null) {
                try {
                    checkSet.close();
                } catch (Exception e) {
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (Exception e) {
                }
            }
        }
        return currentRecords;
    }

    /**
     * Parse pk values object [ ].
     *
     * @param records the records
     * @return the object [ ]
     */
    protected Object[] parsePkValues(TableRecords records) {
        String pkName = records.getTableMeta().getPkName();
        List<Row> undoRows = records.getRows();
        Object[] pkValues = new Object[undoRows.size()];
        for (int i = 0; i < undoRows.size(); i++) {
            List<Field> fields = undoRows.get(i).getFields();
            if (fields != null) {
                for (Field field : fields) {
                    if (StringUtils.equalsIgnoreCase(pkName, field.getName())) {
                        pkValues[i] = field.getValue();
                    }
                }
            }
        }
        return pkValues;
    }
}
