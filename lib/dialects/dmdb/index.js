'use strict';

const _ = require('lodash');
const AbstractDialect = require('../abstract');
const ConnectionManager = require('./connection-manager');
const Query = require('./query');
const QueryGenerator = require('./query-generator');
const DataTypes = require('../../data-types').dmdb;

class DmdbDialect extends AbstractDialect {
  constructor(sequelize) {
    super();
    this.sequelize = sequelize;
    this.connectionManager = new ConnectionManager(this, sequelize);
    this.QueryGenerator = new QueryGenerator({
      _dialect: this,
      sequelize
    });
  }
}

DmdbDialect.prototype.supports = _.merge(_.cloneDeep(AbstractDialect.prototype.supports), {
  'VALUES ()': true,
  'LIMIT ON UPDATE': true,
  lock: true,
  forShare: 'LOCK IN SHARE MODE',
  settingIsolationLevelDuringTransaction: false,
  inserts: {
    ignoreDuplicates: ' IGNORE',
    updateOnDuplicate: ' ON DUPLICATE KEY UPDATE'
  },
  index: {
    collate: false,
    length: true,
    parser: true,
    type: true,
    using: 1
  },
  constraints: {
    dropConstraint: false,
    check: false
  },
  indexViaAlter: false,
  indexHints: true,
  NUMERIC: true,
  GEOMETRY: false,
  JSON: true,
  REGEXP: true,
  
  /* features specific to autoIncrement values */
  autoIncrement: {
    /* does the dialect require modification of insert queries when inserting auto increment fields */
    identityInsert: false,

    /* does the dialect support inserting default/null values for autoincrement fields */
    defaultValue: false,

    /* does the dialect support updating autoincrement fields */
    update: false
  },

  /* does the dialect support returning values for inserted/updated fields */
  returnValues: true
});

ConnectionManager.prototype.defaultVersion = '8.0.0';
DmdbDialect.prototype.Query = Query;
DmdbDialect.prototype.QueryGenerator = QueryGenerator;
DmdbDialect.prototype.DataTypes = DataTypes;
DmdbDialect.prototype.name = 'dmdb';
DmdbDialect.prototype.TICK_CHAR = '`';
DmdbDialect.prototype.TICK_CHAR_LEFT = DmdbDialect.prototype.TICK_CHAR;
DmdbDialect.prototype.TICK_CHAR_RIGHT = DmdbDialect.prototype.TICK_CHAR;

module.exports = DmdbDialect;
