'use strict';

const Utils = require('../../utils');
const AbstractQuery = require('../abstract/query');
const sequelizeErrors = require('../../errors');
const _ = require('lodash');
const { logger } = require('../../utils/logger');

const debug = logger.debugContext('sql:dmdb');


class Query extends AbstractQuery {
  constructor(connection, sequelize, options) {
    super(connection, sequelize, Object.assign({ showWarnings: false }, options));
    this.maxRows = options.maxRows || 0;
    this.outFormat = this.sequelize.connectionManager.lib.OUT_FORMAT_OBJECT;
    this.resultSet = options.resultSet === true;
    this.extendedMetaData = options.extendedMetaData === true;
  }

  static formatBindParameters(sql, values, dialect) {
    const bindParam = [];
    const replacementFunc = (match, key, values) => {
      if (values[key] !== undefined) {
        bindParam.push(values[key]);
        return '?';
      }
      return undefined;
    };
    sql = AbstractQuery.formatBindParameters(sql, values, dialect, replacementFunc)[0];
    return [sql, bindParam.length > 0 ? bindParam : undefined];
  }

  run(sql, parameters) {
    // fix: replace true with 1, false with 0
    if (this.isSelectQuery() && (sql.includes(' = true') || sql.includes(' = false'))) {
      sql = sql.replace(/ = true/g, ' = 1').replace(/ = false/g, ' = 0');
    }
    this.sql = sql;
    const { connection, options } = this;

    //do we need benchmark for this query execution
    const showWarnings = this.sequelize.options.showWarnings || options.showWarnings;

    const complete = this._logQuery(sql, debug, parameters);

    const execOptions = {
      extendedMetaData: this.extendedMetaData,
      outFormat: this.outFormat,
      resultSet: this.resultSet
    };

    return new Utils.Promise((resolve, reject) => {
      const handler = (err, results) => {
        complete();

        if (err) {
          // dmdb automatically rolls-back transactions in the event of a deadlock
          if (options.transaction && err.errno === 1213) {
            options.transaction.finished = 'rollback';
          }
          err.sql = sql;
          err.parameters = parameters;
          reject(this.formatError(err));
        } else {
          resolve(results.rows || results.resultSet || results);
        }
      };
      if (parameters) {
        debug('parameters(%j)', parameters);
        connection.execute(sql, parameters, execOptions, handler);
      } else {
        connection.execute(sql, {}, execOptions, handler);
      }
    })
    // Log warnings if we've got them.
      .then(results => {
        if (showWarnings && results && results.warningStatus > 0) {
          return this.logWarnings(results);
        }
        return results;
      })
    // Return formatted results...
      .then(results => this.formatResults(results));
  }

  /**
   * High level function that handles the results of a query execution.
   *
   *
   * Example:
   *  query.formatResults([
   *    {
   *      id: 1,              // this is from the main table
   *      attr2: 'snafu',     // this is from the main table
   *      Tasks.id: 1,        // this is from the associated table
   *      Tasks.title: 'task' // this is from the associated table
   *    }
   *  ])
   *
   * @param {Array} data - The result of the query execution.
   * @private
   */
  formatResults(data) {
    let result = this.instance;

    if (this.isInsertQuery(data)) {
      this.handleInsertQuery(data);

      if (!this.instance) {
        // handle bulkCreate AI primiary key
        if (
          data.constructor.name === 'ResultSetHeader'
          && this.model
          && this.model.autoIncrementAttribute
          && this.model.autoIncrementAttribute === this.model.primaryKeyAttribute
          && this.model.rawAttributes[this.model.primaryKeyAttribute]
        ) {
          const startId = data[this.getInsertIdField()];
          result = [];
          for (let i = startId; i < startId + data.affectedRows; i++) {
            result.push({ [this.model.rawAttributes[this.model.primaryKeyAttribute].field]: i });
          }
        } else {
          result = data[this.getInsertIdField()];
        }
      }
    }

    if (this.isSelectQuery()) {
      return this.handleSelectQuery(data);
    }
    if (this.isShowTablesQuery()) {
      return this.handleShowTablesQuery(data);
    }
    if (this.isDescribeQuery()) {
      result = {};

      for (const _result of data) {
        const enumRegex = /^enum/i;
        result[_result.Field] = {
          type: enumRegex.test(_result.Type) ? _result.Type.replace(enumRegex, 'ENUM') : _result.Type.toUpperCase(),
          allowNull: _result.Null === 'YES',
          defaultValue: _result.Default,
          primaryKey: _result.Key === 'PRI',
          autoIncrement: Object.prototype.hasOwnProperty.call(_result, 'Extra') && _result.Extra.toLowerCase() === 'auto_increment',
          comment: _result.Comment ? _result.Comment : null
        };
      }
      return result;
    }
    if (this.isShowIndexesQuery()) {
      return this.handleShowIndexesQuery(data);
    }
    if (this.isCallQuery()) {
      return data;
    }
    if (this.isBulkUpdateQuery() || this.isBulkDeleteQuery() || this.isUpsertQuery()) {
      return data.affectedRows;
    }
    if (this.isVersionQuery()) {
      let version = data[0];
      if (version && Array.isArray(version)) {
        version = version[0];
      } else if (typeof version === 'object' && version !== null) {
        version = version.BANNER;
      }
      return version ? version.substr(version.indexOf('V')) : null;
    }
    if (this.isForeignKeysQuery()) {
      return data;
    }
    if (this.isInsertQuery() || this.isUpdateQuery()) {
      return [result, data.affectedRows];
    }
    if (this.isShowConstraintsQuery()) {
      return data;
    }
    if (this.isRawQuery()) {
      return data;
    }

    return result;
  }

  logWarnings(results) {
    return this.run('SHOW WARNINGS').then(warningResults => {
      const warningMessage = `dmdb Warnings (${this.connection.uuid || 'default'}): `;
      const messages = [];
      for (const _warningRow of warningResults) {
        if (_warningRow === undefined || typeof _warningRow[Symbol.iterator] !== 'function') continue;
        for (const _warningResult of _warningRow) {
          if (Object.prototype.hasOwnProperty.call(_warningResult, 'Message')) {
            messages.push(_warningResult.Message);
          } else {
            for (const _objectKey of _warningResult.keys()) {
              messages.push([_objectKey, _warningResult[_objectKey]].join(': '));
            }
          }
        }
      }

      this.sequelize.log(warningMessage + messages.join('; '), this.options);

      return results;
    });
  }

  formatError(err) {
    const errCode = err.errno || err.code;

    switch (errCode) {
      case ER_DUP_ENTRY: {
        const match = err.message.match(/Duplicate entry '([\s\S]*)' for key '?((.|\s)*?)'?$/);
        let fields = {};
        let message = 'Validation error';
        const values = match ? match[1].split('-') : void 0;
        const fieldKey = match ? match[2].split('.').pop() : void 0;
        const fieldVal = match ? match[1] : void 0;
        const uniqueKey = this.model && this.model.uniqueKeys[fieldKey];
        if (uniqueKey) {
          if (uniqueKey.msg) { message = uniqueKey.msg; }
          fields = _.zipObject(uniqueKey.fields, values);
        } else {
          fields[fieldKey] = fieldVal;
        }
        const errors = [];
        _.forOwn(fields, (value, field) => {
          errors.push(new sequelizeErrors.ValidationErrorItem(this.getUniqueConstraintErrorMessage(field), 'unique violation', field, value, this.instance, 'not_unique'));
        });
        return new sequelizeErrors.UniqueConstraintError({ message, errors, parent: err, fields, stack: errStack });
      }
      case ER_ROW_IS_REFERENCED:
      case ER_NO_REFERENCED_ROW: {
        const match = err.message.match(/CONSTRAINT ([`"])(.*)\1 FOREIGN KEY \(\1(.*)\1\) REFERENCES \1(.*)\1 \(\1(.*)\1\)/);
        const quoteChar = match ? match[1] : '`';
        const fields = match ? match[3].split(new RegExp(`${quoteChar}, *${quoteChar}`)) : void 0;
        return new sequelizeErrors.ForeignKeyConstraintError({
          reltype: String(errCode) === String(ER_ROW_IS_REFERENCED) ? 'parent' : 'child',
          table: match ? match[4] : void 0,
          fields,
          value: fields && fields.length && this.instance && this.instance[fields[0]] || void 0,
          index: match ? match[2] : void 0,
          parent: err,
          stack: errStack
        });
      }
      default:
        return new sequelizeErrors.DatabaseError(err, { stack: errStack });
    }
  }

  handleShowIndexesQuery(data) {
    // Group by index name, and collect all fields
    data = data.reduce((acc, item) => {
      if (!(item.Key_name in acc)) {
        acc[item.Key_name] = item;
        item.fields = [];
      }

      acc[item.Key_name].fields[item.Seq_in_index - 1] = {
        attribute: item.Column_name,
        length: item.Sub_part || undefined,
        order: item.Collation === 'A' ? 'ASC' : undefined
      };
      delete item.column_name;

      return acc;
    }, {});

    return _.map(data, item => ({
      primary: item.Key_name === 'PRIMARY',
      fields: item.fields,
      name: item.Key_name,
      tableName: item.Table,
      unique: item.Non_unique !== 1,
      type: item.Index_type
    }));
  }
}

module.exports = Query;
module.exports.Query = Query;
module.exports.default = Query;
