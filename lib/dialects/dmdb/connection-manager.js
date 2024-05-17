'use strict';

const AbstractConnectionManager = require('../abstract/connection-manager');
const SequelizeErrors = require('../../errors');
const { logger } = require('../../utils/logger');
const DataTypes = require('../../data-types').dmdb;
const debug = logger.debugContext('connection:dmdb');
const parserStore = require('../parserStore')('dmdb');
const { promisify } = require('util');

/**
 * dmdb Connection Manager
 *
 * Get connections, validate and disconnect them.
 * AbstractConnectionManager pooling use it to handle dmdb specific connections
 * Use https://www.npmjs.com/package/dmdb to connect with dmdb server
 *
 * @private
 */
class ConnectionManager extends AbstractConnectionManager {
  constructor(dialect, sequelize) {
    sequelize.config.port = sequelize.config.port || 5236;
    super(dialect, sequelize);
    this.lib = this._loadDialectModule('dmdb');
    this.refreshTypeParser(DataTypes);
  }

  _refreshTypeParser(dataType) {
    parserStore.refresh(dataType);
  }

  _clearTypeParser() {
    parserStore.clear();
  }

  static _typecast(field, next) {
    if (parserStore.get(field.type)) {
      return parserStore.get(field.type)(field, this.sequelize.options, next);
    }
    return next();
  }

  /**
   * Connect with dmdb database based on config, Handle any errors in connection
   * Set the pool handlers on connection.error
   * Also set proper timezone once connection is connected.
   *
   * @param {object} config
   * @returns {Promise<Connection>}
   * @private
   */
  async connect(config) {
    // 文档: https://eco.dameng.com/document/dm/zh-cn/pm/nodejs-rogramming-guide.html
    const connectionConfig = {
      connectString: `dm://${config.username}:${config.password}@${config.host}:${config.port}?autoCommit=true`,
      poolMax: config.pool.max,
      poolTimeout: config.pool.idle,
      ...config.dialectOptions
    };
    if (config.database) {
      // 添加模式名到数据库连接串
      connectionConfig.connectString += `&schema=${config.database}`;
    }
    // 指定结果集中的数据类型以 String 显示，取值范围：dmdb.BUFFER、dmdb.CLOB、dmdb.DATE、dmdb.NUMBER
    this.lib.fetchAsString = [this.lib.CLOB];

    try {
      const pool = await this.lib.createPool(connectionConfig);
      const connection = await pool.getConnection();
      debug('connection acquired');
      return connection;
    } catch (err) {
      switch (err.errCode) {
        case 'ECONNREFUSED':
          throw new SequelizeErrors.ConnectionRefusedError(err);
        case 'ER_ACCESS_DENIED_ERROR':
          throw new SequelizeErrors.AccessDeniedError(err);
        case 'ENOTFOUND':
          throw new SequelizeErrors.HostNotFoundError(err);
        case 'EHOSTUNREACH':
          throw new SequelizeErrors.HostNotReachableError(err);
        case 'EINVAL':
          throw new SequelizeErrors.InvalidConnectionError(err);
        default:
          throw new SequelizeErrors.ConnectionError(err);
      }
    }
  }

  async disconnect(connection) {
    // Don't disconnect connections with CLOSED state
    if (connection._closing) {
      debug('connection tried to disconnect but was already at CLOSED state');
      return;
    }

    return await promisify(callback => connection.close(callback))();
  }

  validate(connection) {
    return connection
      && !connection._fatalError
      && !connection._protocolError
      && !connection._closing
      && !connection.closed;
  }
}

module.exports = ConnectionManager;
module.exports.ConnectionManager = ConnectionManager;
module.exports.default = ConnectionManager;
