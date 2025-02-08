'use strict';

const wkx = require('wkx');
const _ = require('lodash');
const momentTz = require('moment-timezone');
const moment = require('moment');

module.exports = BaseTypes => {
  BaseTypes.ABSTRACT.prototype.dialectTypes = 'https://eco.dameng.com/document/dm/zh-cn/sql-dev/dmpl-sql-datatype';

  BaseTypes.DATE.types.dmdb = ['TIMESTAMP'];
  BaseTypes.STRING.types.dmdb = ['VARCHAR'];
  BaseTypes.CHAR.types.dmdb = ['CHAR'];
  BaseTypes.TEXT.types.dmdb = ['TEXT'];
  BaseTypes.TINYINT.types.dmdb = ['TINYINT'];
  BaseTypes.SMALLINT.types.dmdb = ['SMALLINT'];
  BaseTypes.MEDIUMINT.types.dmdb = ['INT'];
  BaseTypes.INTEGER.types.dmdb = ['INTEGER'];
  BaseTypes.BIGINT.types.dmdb = ['BIGINT'];
  BaseTypes.FLOAT.types.dmdb = ['FLOAT'];
  BaseTypes.TIME.types.dmdb = ['TIME'];
  BaseTypes.DATEONLY.types.dmdb = ['DATE'];
  BaseTypes.BOOLEAN.types.dmdb = ['BIT'];
  BaseTypes.BLOB.types.dmdb = ['BLOB'];
  BaseTypes.DECIMAL.types.dmdb = ['DECIMAL'];
  BaseTypes.UUID.types.dmdb = false;
  BaseTypes.ENUM.types.dmdb = false;
  BaseTypes.REAL.types.dmdb = ['DOUBLE'];
  BaseTypes.DOUBLE.types.dmdb = ['DOUBLE'];
  BaseTypes.GEOMETRY.types.dmdb = ['GEOMETRY'];
  BaseTypes.JSON.types.dmdb = ['JSON'];
  BaseTypes.JSONB.types.dmdb = ['JSONB'];

  class DATE extends BaseTypes.DATE {
    toSql() {
      return this._length ? `TIMESTAMP(${this._length})` : 'TIMESTAMP';
    }
    _stringify(date, options) {
      if (!moment.isMoment(date)) {
        date = this._applyTimezone(date, options);
      }
      if (this._length) {
        return date.format(`YYYY-MM-DD HH:mm:ss.${new Array(this._length).fill('S').join('')}`);
      }
      return date.format('YYYY-MM-DD HH:mm:ss');
    }
    static parse(value, options) {
      value = value.string();
      if (value === null) {
        return value;
      }
      if (momentTz.tz.zone(options.timezone)) {
        value = momentTz.tz(value, options.timezone).toDate();
      } else {
        value = new Date(`${value} ${options.timezone}`);
      }
      return value;
    }
  }

  class DATEONLY extends BaseTypes.DATEONLY {
    static parse(value) {
      return value.string();
    }
  }

  class TIME extends BaseTypes.TIME {
    toSql() {
      return `TIME${this._length ? `(${this._length})` : ''}`;
    }
    _stringify(date, options) {
      if (!moment.isMoment(date)) {
        date = this._applyTimezone(date, options);
      }
      if (this._length) {
        return date.format(`HH:mm:ss.${new Array(this._length).fill('S').join('')}`);
      }
      return date.format('HH:mm:ss');
    }
  }

  class UUID extends BaseTypes.UUID {
    toSql() {
      return 'CHAR(36)';
    }
  }

  const SUPPORTED_GEOMETRY_TYPES = ['POINT', 'LINESTRING', 'POLYGON'];
  class GEOMETRY extends BaseTypes.GEOMETRY {
    constructor(type, srid) {
      super(type, srid);
      if (_.isEmpty(this.type)) {
        this.sqlType = this.key;
        return;
      }
      if (SUPPORTED_GEOMETRY_TYPES.includes(this.type)) {
        this.sqlType = this.type;
        return;
      }
      throw new Error(`Supported geometry types are: ${SUPPORTED_GEOMETRY_TYPES.join(', ')}`);
    }
    static parse(value) {
      value = value.buffer();
      if (!value || value.length === 0) {
        return null;
      }
      value = value.slice(4);
      return wkx.Geometry.parse(value).toGeoJSON({ shortCrs: true });
    }
    toSql() {
      return this.sqlType;
    }
  }

  class ENUM extends BaseTypes.ENUM {
    toSql(options) {
      // 获取 ENUM 值最大长度
      const values = this.values.map(value => options.escape(value));
      const enumValueLength = Math.max(...values.map(value => value.length));
      return `CHAR(${enumValueLength})`;
    }
  }

  class JSONTYPE extends BaseTypes.JSON {
    _stringify(value, options) {
      return options.operation === 'where' && typeof value === 'string' ? value : JSON.stringify(value);
    }
    toSql() {
      return 'TEXT';
    }
    static parse(value) {
      if (typeof value === 'string' && value.length > 0) {
        return JSON.parse(value);
      }
      return value;
    }
  }

  class BOOLEAN extends BaseTypes.BOOLEAN {
    toSql() {
      return 'BIT';
    }
  }
  return {
    ENUM,
    DATE,
    DATEONLY,
    TIME,
    UUID,
    GEOMETRY,
    JSON: JSONTYPE,
    JSONB: JSONTYPE,
    BOOLEAN
  };
};
