/*eslint-disable max-lines */
/*eslint-disable consistent-this */
/*eslint-disable radix */
/*eslint-disable class-methods-use-this */
/*eslint-disable valid-jsdoc */

const redis = require('redis');
const uuid = require('node-uuid');
const _u = require('underscore');
const async = require("async");
const conditional = require('async-if-else')({});
const ByteBuffer = require("bytebuffer");

let connectionArray = [];
let connectionCredentials = {};

module.exports = class datalake {
    constructor() {
        this.RedisConnected = false;
        this.maxConnection = 0;
        this.ConnectionInUse = 0;
    }
    CreatePoolConnections(ConnectionObj) {
        return new Promise((resolve, reject) => {
            try {
                connectionArray = [];
                this.maxConnection = ConnectionObj.maxConnection ? parseInt(ConnectionObj.maxConnection, 0) : 10;
                let conn = 0;
                connectionCredentials = ConnectionObj;
                async.whilst(
                    () => {
                        return conn < this.maxConnection;
                    },
                    (callback) => {
                        connectionArray = connectionArray.concat(redis.createClient(ConnectionObj));

                        connectionArray[conn].on('connect', () => {
                            console.log("redis Connected");

                            connectionArray[conn].select(ConnectionObj.dbname, () => {
                                console.log("Redis db " + ConnectionObj.dbname + " selected");
                                conn++;
                                return callback(null);
                            });
                        });
                        connectionArray[conn].on('error', (err) => {
                            console.log(err);
                            return callback(err);
                        });
                    },
                    (err) => {
                        if (err) {
                            console.log(err);
                            return reject(err);
                        }
                        this.RedisConnected = true;
                        return resolve();
                    }
                );
            } catch (error) {
                console.log(error);
            }
        });
    }
    getConnection(iteration) {
        return new Promise((resolve, reject) => {
            try {
                if (this.ConnectionInUse >= parseInt(this.maxConnection, 0) - 1) {
                    this.ConnectionInUse = 0;
                }
                iteration = iteration ? iteration : 0;
                this.ConnectionInUse++;
                if (connectionArray[this.ConnectionInUse].connected) {
                    return resolve(connectionArray[this.ConnectionInUse]);
                } else if (iteration <= this.maxConnection) {
                    this.ConnectionInUse++;
                    iteration = iteration + 1;
                    return resolve(this.redisClient(iteration));
                } else {
                    connectionArray = [];
                    this.CreatePoolConnections(connectionCredentials).
                        then(() => resolve(this.getConnection())).
                        catch((err) => reject(err));
                }
            } catch (error) {
                console.log(error);
                this.RedisConnected = false;
                return reject(error);
            }
        })
    }
    CreateConnection(ConnectionObj) {
        return new Promise((resolve, reject) => {
            this.CreatePoolConnections(ConnectionObj).
                then(() => resolve()).
                catch((err) => reject(err));
        });
    }
    CloseConnection() {
        async.forEachOf(connectionArray, (connections, key, callback) => {
            connections.quit();
            return callback(null);
        }, () => {
            connectionArray = [];
            this.RedisConnected = false;
        });
    }
    ListSchemas() {
        return new Promise((resolve, reject) => {
            var retJSON = {};
            try {
                if (this.RedisConnected) {
                    this.getConnection().then((redisClient) => redisClient.hlen("Index", (err, count) => {
                        this.getConnection().then((redisClient) => redisClient.hscan("Index", 0, 'MATCH', '*', 'count', count, (err, res) => {
                            if (err) {
                                retJSON.Status = 'false';
                                retJSON.Message = err;
                                return resolve(retJSON);
                            }
                            retJSON.Status = 'true';
                            retJSON.Message = 'Success';
                            retJSON.Count = '';
                            retJSON.items = [];
                            for (const item in res[1]) {
                                if (item % 2 === 0) { // index is even
                                    retJSON.items = retJSON.items.concat(res[1][item]);
                                }
                            }
                            retJSON.Count = retJSON.items.length;
                            retJSON.items = retJSON.items.sort()
                            return resolve(retJSON);
                        }));
                    }))
                } else {
                    retJSON.Status = 'false';
                    retJSON.Message = 'Redis not Connected';
                    return resolve(retJSON);
                }
            } catch (error) {
                retJSON.Status = "false";
                retJSON.Message = error;
                console.log({ details: "ListSchemas exception", error: error });
                return reject(retJSON);
            }
        });
    }
    SetCacheData(postData) {
        return new Promise((resolve, reject) => {
            var retJSON = {};
            try {
                if (this.RedisConnected) {
                    var Schema = postData ? postData.Schema : '';
                    var Key = postData ? postData.Key : '';
                    var Data = postData ? postData.Data.toString().trim() : '';
                    var TimeOut = postData ? postData.TimeOut : 10;
                    this.getConnection().then((redisClient) => redisClient.HSET("Cache:" + Schema, Key, Data));
                    this.getConnection().then((redisClient) => redisClient.EXPIRE("Cache:" + Schema, TimeOut));
                    retJSON.Status = 'true';
                    retJSON.Message = 'Success';
                    return resolve(retJSON);
                }
                retJSON.Status = 'false';
                retJSON.Message = 'Redis not Connected';
                return resolve(retJSON);
            } catch (error) {
                console.log(error);
                return reject(error);
            }
        });
    }
    GetCacheData(postData) {
        return new Promise((resolve, reject) => {
            var retJSON = {};
            try {
                var Hash = postData ? postData.Schema : '';
                if (this.RedisConnected) {
                    this.getConnection().then((redisClient) => redisClient.hgetall("Cache:" + Hash, (err, response) => {
                        if (err) {
                            retJSON.Status = 'false';
                            retJSON.Message = err;
                            return resolve(retJSON);
                        }
                        if (response) {
                            var resultArray = [];
                            for (var index in response) { // eslint-disable-line
                                resultArray = resultArray.concat(response[index]);
                            }
                            retJSON.Status = 'true';
                            retJSON.Data = resultArray;
                            return resolve(retJSON);
                        }
                        return resolve('Hash not found in Redis');
                    }));
                }
            } catch (error) {
                console.log(error);
                retJSON.Status = 'false';
                retJSON.Message = error;
                return reject(retJSON);
            }
        });
    }
    ShowConnectionStatus() {
        return new Promise((resolve, reject) => {
            if (this.RedisConnected) {
                return resolve({
                    Status: 'Redis Module Loaded and Ready to Rock and Roll...',
                    Message: 'DataOcean - Licensed by SkunkworxLab, LLC.'
                });
            }
            return reject({
                Status: false,
                Message: 'Redis not Connected'
            });
        });
    }
    CreateTPGUID(Schema, Guid, callback) {
        var myUUID = Guid ? Guid : uuid.v4();
        try {
            if (this.RedisConnected) {
                this.getConnection().then((redisClient) => {
                    redisClient.sadd('Master:' + Schema, myUUID, (err, result) => {
                        return callback(null, myUUID);
                    });
                });
            } else {
                console.log({ details: 'CreateTPGUID', error: 'Redis connection problem' });
                return callback({ error: 'Redis connection problem' });
            }
        } catch (err) {
            console.log({ details: 'CreateTPGUID exception', error: err });
            return callback(null, myUUID);
        }
    }
    getPayloadData(request) {
        try {
            var payloadStr = request;
            var payload = "";
            if (typeof (payloadStr) == "object") {
                payload = payloadStr;
            } else {
                try {
                    payload = JSON.parse(payloadStr);
                } catch (err) {
                    console.log({ details: "getPayloadData exception", error: err });
                    return false;
                }
            }
            return payload;
        } catch (err) {
            console.log({ details: "getPayloadData error", error: err });
            return false;
        }
    }
    SetupSearchIndex(Hash) {
        try {
            if (this.RedisConnected) {
                this.getConnection().then((redisClient) => redisClient.hset(Hash[0], Hash[1], Hash[2]));
            } else {
                console.log({ details: "SetupSearchIndex", error: "Redis connection problem" });
            }
        } catch (err) {
            console.log({ details: "SetupSearchIndex exception", error: err });
        }
    }
    ConfigureSearchIndex(postData) {
        return new Promise((resolve, reject) => {
            var retJSON = {};
            try {
                var payload = this.getPayloadData(postData);

                if (!payload.ShortCodes) {
                    return reject({ Status: false, Message: 'Invalid Postdata' });
                }

                if (this.RedisConnected) {
                    var ShortCodes = (typeof (payload.ShortCodes) == "string") ? payload.ShortCodes : JSON.stringify(payload.ShortCodes);
                    const Hash = ["Index", payload.Schema, ShortCodes];
                    this.SetupSearchIndex(Hash);
                    retJSON.Status = "true";
                    retJSON.Message = "Success";
                } else {
                    retJSON.Status = "false";
                    retJSON.Message = "Redis connection problem";
                    console.log({ details: "ConfigureSearchIndex", error: retJSON });
                }
                return resolve(retJSON);
            } catch (err) {
                retJSON.Status = "false";
                retJSON.Message = err;
                console.log({ details: "ConfigureSearchIndex exception", error: err });
                return resolve(retJSON);
            }
        });
    }
    addRedisData(type, Schema, ShortCode, Value, Guid) {
        try {
            if (type == "string") {
                const SetAdd = ["{SearchIndex}." + Schema + ":" + ShortCode + ":" + Value, Guid];
                this.getConnection().then((redisClient) => redisClient.sadd(SetAdd[0], SetAdd[1]));
            } else if (type == "integer" && !(isNaN(Value))) {
                const zSetAdd = ["{SearchIndex}." + Schema + ":" + ShortCode, Value, Guid]
                this.getConnection().then((redisClient) => redisClient.zadd(zSetAdd[0], zSetAdd[1], zSetAdd[2]));
            } else if (type == "date") {
                var dateValue = new Date(Value);
                dateValue = dateValue.toLocaleString();
                dateValue = dateValue.replace(/[^\w\s]/g, '').replace(/ /g, '').
                    replace(/AM/g, '000');
                if (!(isNaN(dateValue))) {
                    const zSetAdd = ["{SearchIndex}." + Schema + ":" + ShortCode, dateValue, Guid];
                    this.getConnection().then((redisClient) => redisClient.zadd(zSetAdd[0], zSetAdd[1], zSetAdd[2]));
                }
            }
        } catch (err) {
            console.log({ details: "addRedisData exception", error: err });
            console.log(err);
        }
    }
    processMetaData(Schema, Guid, searchHash, MetaData, oldMetaData) {
        try {
            for (var HashInfo of searchHash) {
                if (HashInfo && HashInfo.sc) {
                    var ShortCode = HashInfo.sc.trim();
                    var type = HashInfo.type.trim();
                    var csv = !!HashInfo.csv;
                    if (csv) {
                        for (const Metakey in MetaData) {
                            if (Metakey == ShortCode) {
                                if (oldMetaData && oldMetaData.hasOwnProperty(ShortCode)) {
                                    for (const eachProperty of MetaData[ShortCode]) {
                                        for (const eachOldProperty of oldMetaData[Metakey]) {
                                            if (eachProperty != eachOldProperty) {
                                                if (type == "string") {
                                                    const SetRemove = ["{SearchIndex}." + Schema + ":" + ShortCode + ":" + eachOldProperty.trim(), Guid];
                                                    this.getConnection().then((redisClient) => redisClient.srem(SetRemove[0], SetRemove[1]));
                                                } else if (type == "integer" || type == "date") {
                                                    const SetRemove = ["{SearchIndex}." + Schema + ":" + ShortCode, Guid];
                                                    this.getConnection().then((redisClient) => redisClient.zrem(SetRemove[0], SetRemove[1]));
                                                }
                                                this.addRedisData(type, Schema, ShortCode, eachProperty.trim(), Guid);
                                            }
                                        }
                                    }
                                    Reflect.deleteProperty(oldMetaData, ShortCode);
                                } else {
                                    for (const eachProperty of MetaData[Metakey]) {
                                        this.addRedisData(type, Schema, ShortCode, eachProperty.trim(), Guid);
                                    }
                                }
                            }
                        }
                    } else {
                        for (const Metakey in MetaData) {
                            if (Metakey == ShortCode) {
                                if (oldMetaData && oldMetaData.hasOwnProperty(ShortCode)) {
                                    for (var OldMeta in oldMetaData) {
                                        if (OldMeta == ShortCode) {
                                            if (MetaData[Metakey].trim() != oldMetaData[OldMeta].trim()) {
                                                if (type == "string") {
                                                    const SetRemove = ["{SearchIndex}." + Schema + ":" + ShortCode + ":" + oldMetaData[OldMeta].trim(), Guid];
                                                    this.getConnection().then((redisClient) => redisClient.srem(SetRemove[0], SetRemove[1]));
                                                } else if (type == "integer" || type == "date") {
                                                    const SetRemove = ["{SearchIndex}." + Schema + ":" + ShortCode, Guid];
                                                    this.getConnection().then((redisClient) => redisClient.zrem(SetRemove[0], SetRemove[1]));
                                                }
                                                this.addRedisData(type, Schema, ShortCode, MetaData[Metakey].trim(), Guid);
                                            }
                                            Reflect.deleteProperty(oldMetaData, ShortCode);
                                        }
                                    }
                                } else {
                                    this.addRedisData(type, Schema, ShortCode, MetaData[Metakey].trim(), Guid);
                                }
                            }
                        }
                        if (oldMetaData) {
                            for (const OldValue in oldMetaData) {
                                if (OldValue == ShortCode) {
                                    if (type == "string") {
                                        const SetRemove = ["{SearchIndex}." + Schema + ":" + ShortCode + ":" + OldValue.trim(), Guid];
                                        this.getConnection().then((redisClient) => redisClient.srem(SetRemove[0], SetRemove[1]));
                                    } else if (type == "integer" || type == "date") {
                                        const SetRemove = ["{SearchIndex}." + Schema + ":" + ShortCode, Guid];
                                        this.getConnection().then((redisClient) => redisClient.zrem());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (exp) {
            console.log({ details: "processMetaData", error: exp });
            console.log(exp);
        }
    }
    populateSearchIndex(_Schema, _Guid, _MetaData, Type, callback) {
        try {
            var Guid = "", Schema = "", MetaData = {};
            var missingMandatoryKeys = "";
            if (_Guid) {
                Guid = _Guid.toString();
            } else {
                missingMandatoryKeys = "Guid";
            }
            if (_Schema) {
                Schema = _Schema.toString();
            } else {
                missingMandatoryKeys = (missingMandatoryKeys == "" ? "Schema" : missingMandatoryKeys + ", Schema");
            }
            if (_MetaData) {
                MetaData = _MetaData.toString();
            } else {
                missingMandatoryKeys = (missingMandatoryKeys == "" ? "MetaData" : missingMandatoryKeys + ", MetaData");
            }

            if (missingMandatoryKeys != "") {
                console.log({ details: "populateSearchIndex missingMandatoryKeys", error: missingMandatoryKeys });
                return callback("missingMandatoryKeys : " + missingMandatoryKeys);
            }

            if (this.RedisConnected) {
                async.waterfall([
                    (callback) => {
                        const Hash = ["Index", Schema]
                        this.getConnection().then((redisClient) => redisClient.hget(Hash[0], Hash[1], (err, res) => {
                            var response = '';
                            if (err) {
                                console.log({ details: "populateSearchIndex hget", error: err });
                                return callback(err);
                            }
                            try {
                                response = (typeof (res) == "string") ? JSON.parse(res) : res;
                            } catch (exp) {
                                console.log({ details: "populateSearchIndex response parse : ", error: exp });
                                return callback(exp);
                            }
                            return callback(null, response);
                        }));
                    },
                    (searchHash, callback) => {
                        if (Type && Type == "Refresh") {
                            try {
                                MetaData = (typeof (MetaData) == "string") ? JSON.parse(MetaData) : MetaData;
                            } catch (err) {
                                console.log({ details: "populateSearchIndex parse MetaData", error: err });
                                return callback(err);
                            }
                            if (searchHash) {
                                this.processMetaData(Schema, Guid, searchHash, MetaData, null);
                            }
                            return callback(null);
                        } else { //eslint-disable-line
                            var oldMetaData = "";
                            const GetHash = ["Data:" + Schema, Guid]
                            this.getConnection().then((redisClient) => {
                                redisClient.hget(GetHash[0], GetHash[1], (err, res) => {
                                    if (err) {
                                        console.log({ details: "populateSearchIndex hget", error: err });
                                        return callback(err);
                                    }
                                    const SetHash = ["Data:" + Schema, Guid, MetaData];
                                    this.getConnection().then((redisClient) => redisClient.hset(SetHash[0], SetHash[1], SetHash[2]));
                                    try {
                                        MetaData = (typeof (MetaData) == "string") ? JSON.parse(MetaData) : MetaData;
                                    } catch (exp) {
                                        console.log({ details: "populateSearchIndex parse MetaData", error: exp });
                                        return callback(exp);
                                    }
                                    if (res) {
                                        try {
                                            oldMetaData = (typeof (res) == "string") ? JSON.parse(res) : res;
                                        } catch (exp) {
                                            console.log({ details: "populateSearchIndex parse oldMetaData", error: exp });
                                            return callback(exp);
                                        }
                                    }
                                    if (searchHash) {
                                        this.processMetaData(Schema, Guid, searchHash, MetaData, oldMetaData);
                                    }
                                    return callback(null);
                                });
                            });
                        }
                    }
                ], (err) => {
                    if (err) {
                        console.log({ details: "populateSearchIndex waterfall", error: err });
                        return callback(err);
                    }
                    return callback(null);
                });
            } else {
                console.log({ details: "populateSearchIndex", error: "Redis connection problem" });
                return callback("Redis connection problem");
            }
        } catch (err) {
            console.log({ details: "populateSearchIndex exception", error: err });
            return callback(err);
        }
    }
    dataInsert(Schema, Guid, MetaData, Tag, Comment, Action, retJSON, callback) {
        try {
            const get = ['Data:' + Schema, Guid];
            this.getHash(get[0], get[1], (err, response) => {
                if (err == 'Hash Key not found in Redis') {
                    this.populateSearchIndex(Schema, Guid, MetaData, null, (err) => {
                        if (err) {
                            retJSON.Status = "false";
                            retJSON.Message = err;
                        } else {
                            retJSON.Status = "true";
                            retJSON.Message = "Data Inserted Successfully";
                            retJSON.Guid = Guid;
                        }
                        return callback(null, retJSON);
                    });
                } else {
                    return callback(err);
                }
                if (response) {
                    this.snapShotSchemaData(Schema, Guid, Tag, Comment, Action, (err, res) => {
                        if (err) {
                            retJSON.Status = "false";
                            retJSON.Message = err;
                            return callback(null, retJSON);
                        }
                        this.populateSearchIndex(Schema, Guid, MetaData, null, (err) => {
                            if (err) {
                                retJSON.Status = "false";
                                retJSON.Message = err;
                            } else {
                                retJSON.Status = "true";
                                retJSON.Message = "Success";
                                retJSON.Action = "Data Updated, Backup Version: " + res;
                            }
                            return callback(null, retJSON);
                        });
                    });
                }
            });
        } catch (error) {
            console.log(error);
            return callback(error);
        }
    }
    getHash(Hash, Key, callback) {
        try {
            if (this.RedisConnected) {
                this.getConnection().then((redisClient) => {
                    redisClient.hget(Hash, Key, (err, res) => {
                        if (err) {
                            return callback(err);
                        }
                        if (res) {
                            return callback(null, res);
                        }
                        return callback('Hash Key not found in Redis', null);
                    });
                });
            } else {
                return callback('Redis connection problem');
            }
        } catch (err) {
            console.log({ details: "getHash exception", error: err });
            return callback(err);
        }
    }
    formatDate(inputDate) {
        try {
            var searchDT = new Date(inputDate);
            searchDT = searchDT.toLocaleString();
            searchDT = searchDT.replace(/[^\w\s]/g, '').replace(/ /g, '').
                replace(/AM/g, '000');
            return searchDT;
        } catch (err) {
            console.log({ details: "formatDate exception", error: err });
            return "";
        }
    }
    getResult(SearchListString, SearchLOVString, SearchListScore, SearchLOVScore, callback) {
        try {
            var GuidList = [];
            var hit1 = false;
            var hit2 = false;
            async.parallel([
                (callback) => {
                    if (SearchListString && SearchListString.length > 0) {
                        this.getConnection().then((redisClient) => {
                            redisClient.sinter(SearchListString, (err, res) => {
                                hit1 = true;
                                if (err) {
                                    console.log({ details: "sinter Error", error: err });
                                    return callback(err);
                                }
                                if (res) {
                                    return callback(null, res);
                                }
                                return callback('Hash Key not found in Redis');
                            });
                        });
                    } else {
                        return callback(null);
                    }
                },
                (callback) => {
                    var ScoreResult = [];
                    async.forEachOf(SearchListScore, (ScoreCommand, i, callback) => {
                        var searchScoreCommand = ScoreCommand.split(',');
                        this.getConnection().then((redisClient) => {
                            redisClient.zrangebyscore(searchScoreCommand, (err, res) => {
                                hit2 = true;
                                if (err) {
                                    console.log({ details: "zrangebyscore Error", error: err });
                                    return callback(err);
                                }
                                if (res) {
                                    if (ScoreResult.length == 0) {
                                        ScoreResult = res;
                                    } else {
                                        ScoreResult = _u.intersection(ScoreResult, res);
                                    }
                                }
                                return callback(null);
                            });
                        });

                    }, (err) => {
                        if (err) {
                            console.log({ details: "zrangebyscore exception", error: err });
                            return callback(err);
                        }
                        return callback(null, ScoreResult);
                    });
                }
            ], (err, results) => {
                if (err) {
                    console.log({ details: "getResult error", error: err });
                    return callback(err);
                }
                if (hit1 && hit2) {
                    GuidList = _u.intersection(results[0], results[1]);
                    return callback(null, GuidList, SearchListString, SearchLOVString, SearchListScore, SearchLOVScore);
                } else if (hit1) {
                    GuidList = results[0];
                    return callback(null, GuidList, SearchListString, SearchLOVString, SearchListScore, SearchLOVScore);
                } else if (hit2) {
                    GuidList = results[1];
                    return callback(null, GuidList, SearchListString, SearchLOVString, SearchListScore, SearchLOVScore);
                }
                return callback(null, null, SearchListString, SearchLOVString, SearchListScore, SearchLOVScore);
            });
        } catch (exp) {
            console.log({ details: "getResult exception", error: exp });
            return callback(exp);
        }
    }
    getResultLOV(preGuidList, SearchListString, SearchLOVString, SearchListScore, SearchLOVScore, callback) {
        try {
            var GuidList = [];
            var hitfn1 = false;
            var hitfn2 = false;
            async.parallel([
                (callback) => {
                    // var ScoreLOVResult = [];
                    async.forEachOf(SearchLOVString, (command, i, callback) => {
                        var searchCommand = command.split(',');
                        this.getConnection().then((redisClient) => {
                            redisClient.sunion(searchCommand, (err, res) => {
                                hitfn1 = true;
                                if (err) {
                                    console.log({ details: "sunion Error", error: err });
                                    return callback(err);
                                }
                                if (res) {
                                    if (GuidList.length == 0) {
                                        GuidList = res;
                                    } else {
                                        GuidList = _u.intersection(GuidList, res);
                                    }
                                }
                                return callback(null);
                            });
                        });
                    }, (err) => {
                        if (err) {
                            console.log({ details: "sunion exception", error: err });
                            return callback(err);
                        }
                        return callback(null, GuidList);
                    });
                },
                (callback) => {
                    var TotalScoreResult = [];
                    async.forEachOf(SearchLOVScore, (ScoreCommandList, i, callback) => {
                        hitfn2 = true;
                        var ScoreResult = [];
                        async.forEachOf(ScoreCommandList, (ScoreCommand, i, cb) => {
                            var ScoreList = ScoreCommand.split(',');
                            this.getConnection().then((redisClient) => {
                                redisClient.zrangebyscore(ScoreList, (err, res) => {
                                    if (err) {
                                        console.log({ details: "zrangebyscore Error", error: err });
                                        return cb(err);
                                    }
                                    if (res) {
                                        if (ScoreResult.length == 0) {
                                            ScoreResult = res;
                                        } else {
                                            ScoreResult = _u.union(ScoreResult, res);
                                        }
                                    }
                                    return cb(null);
                                });
                            });
                        }, (err) => {
                            if (err) {
                                console.log({ details: "zrangebyscore exception", error: err });
                                return callback(err);
                            }
                            if (TotalScoreResult.length == 0) {
                                TotalScoreResult = ScoreResult;
                            } else {
                                TotalScoreResult = _u.intersection(TotalScoreResult, ScoreResult);
                            }
                            return callback(null);
                        });
                    }, (err) => {
                        if (err) {
                            console.log({ details: "zrangebyscore exception", error: err });
                            return callback(err);
                        }
                        return callback(null, TotalScoreResult);
                    });
                }
            ], (err, results) => {
                if (err) {
                    console.log({ details: "getResult_LOV error", error: err });
                    return callback(err);
                }
                if (hitfn1 && hitfn2) {
                    GuidList = _u.intersection(results[0], results[1]);
                } else if (hitfn1) {
                    GuidList = results[0];
                } else if (hitfn2) {
                    GuidList = results[1];
                }

                if (GuidList.length > 0 && preGuidList) {
                    GuidList = _u.intersection(GuidList, preGuidList);
                } else if (preGuidList) {
                    GuidList = preGuidList;
                }
                return callback(null, GuidList);
            });
        } catch (exp) {
            console.log({ details: "getResult_LOV exception", error: exp });
            return callback(exp);
        }
    }
    getSearchData(GuidList, Schema, callback) {
        try {
            var items = [];
            async.forEachOf(GuidList, (Guid, i, callback) => {
                const Hash = "Data:" + Schema;
                this.getHash(Hash, Guid, (err, response) => {
                    if (err) {
                        console.log({ details: "getSearchData Error", error: err });
                        return callback(err);
                    }
                    var item = { Guid: Guid, MetaData: response };
                    items.push(item);
                    return callback(null);
                });

            }, (err) => {
                if (err) {
                    console.log({ details: "getSearchData exception", error: err });
                    return callback(err);
                }
                return callback(null, items);
            });
        } catch (exp) {
            console.log({ details: "getSearchData exception", error: exp });
            return callback(exp);
        }
    }
    SearchTPHash(Schema, payload, resultArray, callback) {
        try {
            var keys = Object.keys(payload);
            var SearchListString = [];
            var SearchListScore = [];
            var SearchLOVString = [];
            var SearchLOVScore = [];
            async.waterfall([
                (callback) => {
                    this.getHash("Index", Schema, callback);
                },
                (TpSearchHash, callback) => {
                    var TpSearchHashJson = {};
                    try {
                        TpSearchHashJson = (typeof (TpSearchHash) == "string") ? JSON.parse(TpSearchHash) : TpSearchHash;
                    } catch (err) {
                        console.log({ details: "SearchTPHash", error: err });
                        return callback(err);
                    }

                    for (var HashInfo of TpSearchHashJson) {
                        if (HashInfo && HashInfo.sc) {
                            var ShortCode = HashInfo.sc;
                            var type = HashInfo.type;
                            for (var i = 0; i < keys.length; i++) {
                                var SearchShortCode = keys[i];
                                var SearchValue = payload[keys[i]];
                                if (SearchShortCode != "Keyword" && ShortCode == SearchShortCode) {
                                    var Search = "";
                                    if (type == "string") {
                                        if (SearchValue.indexOf(',') > -1) {
                                            var LOV = SearchValue.split(',');
                                            var lovKey = '';
                                            for (let j = 0; j < LOV.length; j++) {
                                                lovKey += "{SearchIndex}." + Schema + ":" + SearchShortCode.trim() + ":" + LOV[j].toString().trim() + ',';
                                            }
                                            lovKey = lovKey.slice(0, -1);
                                            SearchLOVString.push(lovKey);
                                        } else {
                                            Search = "{SearchIndex}." + Schema + ":" + SearchShortCode.trim() + ":" + SearchValue.trim();
                                            SearchListString.push(Search);
                                        }
                                    } else if (type == "integer") {
                                        if (SearchValue.indexOf(',') > -1) {
                                            const LOVScore = SearchValue.split(',');
                                            const LOVScoreInternal = [];
                                            for (let j = 0; j < LOVScore.length; j++) {
                                                const lovKeyScore = "{SearchIndex}." + Schema + ":" + SearchShortCode + "," + LOVScore[j].trim() + "," + LOVScore[j].trim();
                                                LOVScoreInternal.push(lovKeyScore);
                                            }
                                            SearchLOVScore.push(LOVScoreInternal);
                                        } else {
                                            const Values = SearchValue.split('~');
                                            if (Values.length > 1) {
                                                Search = "{SearchIndex}." + Schema + ":" + SearchShortCode + "," + Values[0].trim() + "," + Values[1].trim();
                                            } else {
                                                Search = "{SearchIndex}." + Schema + ":" + SearchShortCode + "," + Values[0].trim() + "," + Values[0].trim();
                                            }
                                            SearchListScore.push(Search);
                                        }
                                    } else if (type == "date") {
                                        if (SearchValue.indexOf(',') > -1) {
                                            var LOVScore = SearchValue.split(',');
                                            var LOVScoreInternal = [];
                                            for (var j = 0; j < LOVScore.length; j++) {
                                                var searchDT = this.formatDate(LOVScore[j]);
                                                var lovKeyScore = "{SearchIndex}." + Schema + ":" + SearchShortCode + "," + searchDT.trim() + "," + searchDT.trim();
                                                LOVScoreInternal.push(lovKeyScore);
                                            }
                                            SearchLOVScore.push(LOVScoreInternal);
                                        } else {
                                            var Values = SearchValue.split('~');
                                            var fromDate = this.formatDate(Values[0]);
                                            if (Values.length > 1) {
                                                var toDate = this.formatDate(Values[1]);
                                                Search = "{SearchIndex}." + Schema + ":" + SearchShortCode + "," + fromDate.trim() + "," + toDate.trim();
                                            } else {
                                                Search = "{SearchIndex}." + Schema + ":" + SearchShortCode + "," + fromDate.trim() + "," + fromDate.trim();
                                            }
                                            SearchListScore.push(Search);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    return callback(null, SearchListString, SearchLOVString, SearchListScore, SearchLOVScore);
                },
                (SearchListString, SearchLOVString, SearchListScore, SearchLOVScore, callback) => this.getResult(SearchListString, SearchLOVString, SearchListScore, SearchLOVScore, callback),
                (preGuidList, SearchListString, SearchLOVString, SearchListScore, SearchLOVScore, callback) => this.getResultLOV(preGuidList, SearchListString, SearchLOVString, SearchListScore, SearchLOVScore, callback),
                (GuidList, callback) => {
                    this.getSearchData(GuidList, Schema, callback);
                }
            ], (err, result) => {
                if (err) {
                    console.log('SearchTPHash error', err);
                    result = result ? result : [];
                    // return callback(err);
                }
                if (resultArray.length == 0) {
                    resultArray[Schema] = resultArray.concat(result);
                } else {
                    resultArray[Schema] = resultArray[Schema].concat(result);
                }
                return callback(null, result);
            });
        } catch (err) {
            console.log({ details: "SearchTPHash exception", error: err });
            return callback(err);
        }
    }
    SearchDataByProperty(postData) {
        return new Promise((resolve, reject) => {
            var retJSON = {};
            try {
                var payload = this.getPayloadData(postData);

                if (!payload) {
                    return reject({ Status: false, Message: 'Invalid Postdata' });
                }

                var Schema = payload.Schema;
                var resultArray = [];

                this.SearchTPHash(Schema, payload, resultArray, (err, result) => {
                    if (err) {
                        retJSON.Status = "false";
                        retJSON.Message = err;
                    } else {
                        retJSON.Status = "true";
                        retJSON.Message = "Success";
                        retJSON.TotalCount = result.length;
                        retJSON.items = result;
                    }
                    return resolve(retJSON);
                });
            } catch (err) {
                retJSON.Status = "false";
                retJSON.Message = err;
                console.log({ details: "SearchDataByProperty exception", error: err });
                return resolve(retJSON);
            }
        });
    }
    getHashAll(Hash, callback) {
        try {
            if (this.RedisConnected) {
                this.getConnection().then((redisClient) => {
                    redisClient.hgetall(Hash, (err, res) => {
                        if (err) {
                            return callback(err);
                        }
                        if (res) {
                            return callback(null, res);
                        }
                        return callback('Hash not found in Redis');
                    });
                });
            } else {
                return callback('Redis connection problem');
            }
        } catch (err) {
            console.log({ details: "getHashAll exception", error: err });
            return callback(err);
        }
    }
    /*     sscanSearch(Schema, finalResult, Nextbatch, callback) {
            this.getConnection().then((redisClient) => redisClient.sscan("Master:" + Schema, Nextbatch, 'MATCH', '*', (err, result) => {
                if (err) {
                    console.log('err:', err);
                    return callback(err);
                }
                for (const distinct of result[1]) {
                    finalResult = finalResult.concat(distinct); // eslint-disable-line
                }
                if (result[0] == 0) {
                    return callback(null, finalResult);
                }
                this.sscanSearch(Schema, finalResult, result[0], callback);
            }));
        } */
    GetAllSchemaData(postData) {
        return new Promise((resolve, reject) => {
            var retJSON = {};
            var items = [];
            try {
                var payload = this.getPayloadData(postData);

                if (!payload) {
                    return reject({ Status: false, Message: 'Invalid Postdata' });
                }

                var Schema = "", Guid = '';
                var missingMandatoryKeys = "";
                if (payload.Schema) {
                    Schema = payload.Schema.toString();
                } else {
                    missingMandatoryKeys = "Schema";
                }
                if (payload.Guid) {
                    Guid = payload.Guid.toString();
                }

                if (missingMandatoryKeys != "") {
                    retJSON.Status = "false";
                    retJSON.Message = "missingMandatoryKeys : " + missingMandatoryKeys;
                    console.log({ details: "GetAllSchemaData missingMandatoryKeys", error: missingMandatoryKeys });
                    return resolve(retJSON);
                }

                if (this.RedisConnected) {
                    if (Guid) {
                        this.getHash("Data:" + Schema, Guid, (err, result) => {
                            if (err) {
                                console.log(err);
                                retJSON.Status = "false";
                                retJSON.Message = err;
                            } else {
                                retJSON.Status = "true";
                                retJSON.Message = "Success";
                                items.push({ Guid: Guid, MetaData: result });
                                retJSON.items = items;
                            }
                            return resolve(retJSON);
                        });
                    } else {
                        this.getHashAll("Data:" + Schema, (err, result) => {
                            if (err) {
                                console.log(err);
                                retJSON.Status = "false";
                                retJSON.Message = err;
                            } else if (result) {
                                retJSON.Status = "true";
                                retJSON.Message = "Success";
                                var keys = Object.keys(result);
                                for (var i = 0; i < keys.length; i++) {
                                    var retObj = {};
                                    retObj.Guid = keys[i];
                                    retObj.MetaData = result[keys[i]];;
                                    items.push(retObj);
                                }
                                retJSON.items = items;
                            }
                            return resolve(retJSON);
                        });
                    }
                    /* async.waterfall([
                        (callback) => {
                            if (Guid && this.flagLabel) {
                                this.getConnection().then((redisClient) => redisClient.sismember("Master:" + Schema, Guid, (err, res) => {
                                    if (err) {
                                        return callback(err);
                                    }
                                    if (res && res == 1) {
                                        return callback(null, [Guid]);
                                    }
                                    return callback('Posted Schema not found in TpSchemaSet');
                                }));
                            } else {
                                this.sscanSearch(Schema, [], 0, callback);
                            }
                        },
                        (GuidList, callback) => {
                            if (Keyword) {
                                async.forEachOf(GuidList, (Guid, i, callback) => {
                                    this.getHash("Data:" + Schema + ":" + Guid, Keyword, (err, res) => {
                                        if (err) {
                                            console.log({ details: "GetAllSchemaData:getHash:Error", error: err });
                                        } else {
                                            var retObj = {};
                                            retObj.Guid = Guid;
                                            retObj.Keyword = Keyword;
                                            retObj.SchemaData = res;
                                            items.push(retObj);
                                        }
                                        callback(null);
                                    });
                                }, (err) => {
                                    if (err) {
                                        console.log({ details: "GetAllSchemaData exception", error: err });
                                        return callback(err);
                                    }
                                    return callback(null);
                                });
                            } else {
                                async.forEachOf(GuidList, (Guid, i, callback) => {
                                    this.getHashAll("Data:" + Schema + ":" + Guid, (err, res) => {
                                        if (err) {
                                            console.log({ details: "GetAllSchemaData:getHashAll:Error", error: err });
                                        } else if (res) {
                                            var keys = Object.keys(res);
                                            for (var i = 0; i < keys.length; i++) {
                                                var retObj = {};
                                                var Keyword = keys[i];
                                                var MetaData = res[keys[i]];
                                                retObj.Guid = Guid;
                                                retObj.Keyword = Keyword;
                                                retObj.SchemaData = MetaData;
                                                items.push(retObj);
                                            }
                                        }
                                        callback(null);
                                    });

                                }, (err) => {
                                    if (err) {
                                        console.log({ details: "GetAllSchemaData exception", error: err });
                                        return callback(err);
                                    }
                                    return callback(null);
                                });
                            }
                        }
                    ], (err) => {
                        if (err) {
                            retJSON.Status = "false";
                            retJSON.Message = err;
                        } else {
                            retJSON.Status = "true";
                            retJSON.Message = "Success";
                            retJSON.items = items;
                        }
                        return resolve(retJSON);
                    }); */

                } else {
                    retJSON.Status = "false";
                    retJSON.Message = "Redis connection problem";
                    console.log({ details: "GetAllSchemaData", error: retJSON });
                    return resolve(retJSON);
                }
            } catch (err) {
                retJSON.Status = "false";
                retJSON.Message = err;
                console.log({ details: "GetAllSchemaData exception", error: err });
                return resolve(retJSON);
            }
        });
    }
    snapShotSchemaData(Schema, Guid, Tag, Comment, Action, callback) {
        try {
            if (this.RedisConnected) {
                async.waterfall([
                    (callback) => {
                        const GetAllHash = ["Data:" + Schema];
                        this.getHashAll(GetAllHash[0], (err, res) => {
                            if (err) {
                                console.log({ details: "snapShotSchemaData:getHashAll:Error", error: err });
                                return callback(err);
                            } else if (res) {
                                res.Tag = Tag;
                                res.Comment = Comment;
                                res.Action = Action;
                                return callback(null, res);
                            }
                            return callback('Hash key not found in Redis');
                        });
                    },
                    (DataHash, callback) => {
                        this.getConnection().then((redisClient) => {
                            redisClient.zrevrangebyscore("DataArchive:" + Schema + ":" + Guid, '+inf', '-inf', 'WITHSCORES', 'LIMIT', '0', '1', (err, res) => {
                                if (err) {
                                    console.log({ details: "snapShotSchemaData:zrevrangebyscore:Error", error: err });
                                    return callback(err);
                                } else if (res) {
                                    var Version = "";
                                    if (res && res.length > 0) {
                                        Version = parseInt(res[1]) + 1;
                                    } else {
                                        Version = 1;
                                    }
                                    DataHash.Version = Version.toString();
                                    return callback(null, Version, DataHash);
                                }
                                return callback('Hash key not found in Redis');
                            });
                        });
                    },
                    (Version, InDataHash, callback) => {
                        var DataHash = '';
                        DataHash = (typeof (InDataHash) == "string") ? InDataHash : JSON.stringify(InDataHash);
                        this.getConnection().then((redisClient) => redisClient.zadd("DataArchive:" + Schema + ":" + Guid, Version, DataHash));
                        return callback(null, Version);
                    }
                ], (err, result) => {
                    if (err) {
                        console.log({ details: "snapShotSchemaData:Error", error: err });
                        return callback(err);
                    }
                    return callback(null, result);
                });

            } else {
                console.log({ details: "snapShotSchemaData ", error: "Redis connection problem" });
                return callback("Redis connection problem");
            }
        } catch (exp) {
            console.log({ details: "snapShotSchemaData Exception", error: exp });
            return callback(exp);
        }
    }
    CreateBackupData(postData) {
        return new Promise((resolve, reject) => {
            var retJSON = {};
            try {
                var payload = this.getPayloadData(postData);

                if (!payload) {
                    return reject({ Status: false, Message: 'Invalid Postdata' });
                }

                var Schema = "", Guid = "", Tag = "", Comment = "", Action = "Snapshot";
                var missingMandatoryKeys = "";
                if (payload.Schema) {
                    Schema = payload.Schema.toString();
                } else {
                    missingMandatoryKeys = "Schema";
                }
                if (payload.Guid) {
                    Guid = payload.Guid.toString();
                } else {
                    missingMandatoryKeys = (missingMandatoryKeys == "" ? "Guid" : missingMandatoryKeys + ", Guid");
                }
                Tag = payload.Tag;
                Comment = payload.Comment;

                if (missingMandatoryKeys != "") {
                    retJSON.Status = "false";
                    retJSON.Message = "missingMandatoryKeys : " + missingMandatoryKeys;
                    console.log({ details: "CreateBackupData missingMandatoryKeys", error: missingMandatoryKeys });
                    return resolve(retJSON);
                }

                this.snapShotSchemaData(Schema, Guid, Tag, Comment, Action, (err, res) => {
                    if (err) {
                        retJSON.Status = "false";
                        retJSON.Message = err;
                    } else {
                        retJSON.Status = "true";
                        retJSON.Message = "Success";
                        retJSON.Version = res.toString();
                    }
                    return resolve(retJSON);
                });

            } catch (err) {
                retJSON.Status = "false";
                retJSON.Message = err;
                console.log({ details: "CreateBackupData exception", error: err });
                return resolve(retJSON);
            }
        });
    }
    removeDataHash(_Schema, _Guid, _MetaData, callback) {
        try {
            var Schema = "", Guid = "", MetaData = "";
            var missingMandatoryKeys = "";
            if (_Schema) {
                Schema = _Schema.toString();
            } else {
                missingMandatoryKeys = "Schema";
            }
            if (_Guid) {
                Guid = _Guid.toString();
            } else {
                missingMandatoryKeys = (missingMandatoryKeys == "" ? "Guid" : missingMandatoryKeys + ", Guid");
            }
            if (_MetaData) {
                MetaData = _MetaData.toString();
            } else {
                missingMandatoryKeys = (missingMandatoryKeys == "" ? "MetaData" : missingMandatoryKeys + ", MetaData");
            }

            if (missingMandatoryKeys != "") {
                console.log({ details: "RemoveData missingMandatoryKeys", error: missingMandatoryKeys });
                return callback("missingMandatoryKeys : " + missingMandatoryKeys);
            }
            if (this.RedisConnected) {
                async.waterfall([
                    (callback) => {
                        const Hash = "Index";
                        const Key = Schema;
                        this.getHash(Hash, Key, (err, res) => {
                            if (err) {
                                console.log({ details: "RemoveData TpSearchHash getHash", error: err });
                                return callback(err);
                            }
                            var response = '';
                            try {
                                response = (typeof (res) == "string") ? JSON.parse(res) : res;
                            } catch (exp) {
                                console.log({ details: "RemoveData TpSearchHash response parse : ", error: exp });
                                return callback(exp);
                            }
                            return callback(null, response, MetaData);
                        });
                    },
                    (InSearchHash, InMetaData, callback) => {
                        var MetaData = '', searchHash = '';
                        try {
                            MetaData = (typeof (InMetaData) == "string") ? JSON.parse(InMetaData) : InMetaData;
                            searchHash = (typeof (InSearchHash) == "string") ? JSON.parse(InSearchHash) : InSearchHash;
                        } catch (err) {
                            console.log({ details: "RemoveData MetaData parse : ", error: err });
                            return callback(err);
                        }
                        for (var HashInfo of searchHash) {
                            if (HashInfo && HashInfo.sc) {
                                var ShortCode = HashInfo.sc;
                                var type = HashInfo.type;
                                var self = this;
                                for (const Value in MetaData) {
                                    if (Value == ShortCode) {
                                        if (type == "string") {
                                            this.getConnection().then((redisClient) => {
                                                const Hash = "{SearchIndex}." + Schema + ":" + ShortCode + ":" + Value
                                                redisClient.srem(Hash, Guid);
                                            });
                                        } else if (type == "integer" || type == "date") {
                                            this.getConnection().then((redisClient) => {
                                                const Hash = "{SearchIndex}." + Schema + ":" + ShortCode;
                                                redisClient.zrem(Hash, Guid);
                                            });
                                        }
                                    }
                                }
                            }
                        }
                        return callback(null);
                    }
                ], (err) => {
                    if (err) {
                        console.log(err);
                        return callback(null);
                    }
                    return callback(null);
                });

            } else {
                console.log({ details: "RemoveData ", error: "Redis connection problem" });
                return callback("Redis connection problem");
            }
        } catch (err) {
            console.log({ details: "RemoveData exception", error: err });
            return callback(err);
        }
    }
    RemoveData(postData) {
        return new Promise((resolve, reject) => {
            var retJSON = {};
            try {
                var payload = this.getPayloadData(postData);

                if (!payload) {
                    return reject({ Status: false, Message: 'Invalid Postdata' });
                }

                var Schema = "", Guid = "", Tag = "", Comment = "", Action = "Remove";
                var missingMandatoryKeys = "";
                if (payload.Schema) {
                    Schema = payload.Schema.toString();
                } else {
                    missingMandatoryKeys = "Schema";
                }
                if (payload.Guid) {
                    Guid = payload.Guid.toString();
                } else {
                    missingMandatoryKeys = (missingMandatoryKeys == "" ? "Guid" : missingMandatoryKeys + ", Guid");
                }

                if (missingMandatoryKeys != "") {
                    retJSON.Status = "false";
                    retJSON.Message = "missingMandatoryKeys : " + missingMandatoryKeys;
                    console.log({ details: "RemoveData missingMandatoryKeys", error: missingMandatoryKeys });
                    return resolve(retJSON);
                }
                if (this.RedisConnected) {
                    async.waterfall([
                        (callback) => {
                            this.snapShotSchemaData(Schema, Guid, Tag, Comment, Action, callback);
                        },
                        (Version, callback) => {
                            const Hash = "Data:" + Schema;
                            const Key = Guid;
                            this.getHash(Hash, Key, (err, res) => {
                                if (err) {
                                    console.log({ details: "RemoveData:getHash:Error", error: err });
                                    return callback(err);
                                }
                                if (res) {
                                    this.getConnection().then((redisClient) => redisClient.hdel("Data:" + Schema, Guid));
                                    return callback(null, res);
                                }
                                return callback('Hash key not found in Redis');
                            });
                        },
                        (MetaData, callback) => {
                            this.removeDataHash(Schema, Guid, MetaData, callback);
                        }
                    ], (err) => {
                        if (err) {
                            retJSON.Status = "false";
                            retJSON.Message = err;
                        } else {
                            retJSON.Status = "true";
                            retJSON.Message = "Success";
                        }
                        return resolve(retJSON);
                    });

                } else {
                    retJSON.Status = "false";
                    retJSON.Message = "Redis connection problem";
                    console.log({ details: "RemoveData ", error: "Redis connection problem" });
                    return resolve(retJSON);
                }
            } catch (err) {
                retJSON.Status = "false";
                retJSON.Message = err;
                console.log({ details: "RemoveData exception", error: err });
                return resolve(retJSON);
            }
        });
    }
    checkTPData(Schema, Guid, Tag, Comment, Action, RollbackVersion, self, callback) {
        try {
            if (self.RedisConnected) {
                self.getConnection().then((redisClient) => {
                    redisClient.hget("Data:" + Schema, Guid, (err, res) => {
                        if (err) {
                            return callback(err);
                        }
                        if (res) {
                            return callback(null, true);
                        }
                        return callback(null, false);
                    });
                });
            } else {
                return callback('Redis connection problem');
            }
        } catch (exp) {
            console.log({ details: "getHashAll exception", error: exp });
            return callback(exp);
        }
    }
    snapShotData(Schema, Guid, Tag, Comment, Action, RollbackVersion, self, callback) {
        try {
            async.waterfall([
                (callback) => {
                    self.snapShotSchemaData(Schema, Guid, Tag, Comment, Action, callback);
                },
                (Version, callback) => {
                    self.getHash("Data:" + Schema, Guid, (err, res) => {
                        if (err) {
                            console.log({ details: "RestoreData, snapShotData:getHashAll:Error", error: err });
                            return callback(err);
                        } else if (res) {
                            var items = [];
                            var keys = Object.keys(res);
                            for (var i = 0; i < keys.length; i++) {
                                var retObj = {};
                                var MetaData = res[keys[i]];
                                retObj.Schema = Schema;
                                retObj.Guid = Guid;
                                retObj.MetaData = MetaData;
                                items.push(retObj);
                            }
                            return callback(null, items);
                        }
                        return callback('Hash Key not found in Redis');
                    });
                },
                (DataItems, callback) => {
                    async.forEachOf(DataItems, (item, i, callback) => {
                        self.removeDataHash(item.Schema, item.Guid, item.MetaData, (err) => {
                            if (err) {
                                console.log({ details: "RestoreData, snapShotData:RemoveData:Error", error: err });
                            }
                            callback(null);
                        });
                    }, (err) => {
                        if (err) {
                            console.log({ details: "RestoreData, snapShotTPData forEachOf Error", error: err });
                            return callback(err);
                        }
                        self.getConnection().then((redisClient) => redisClient.hdel("Data:" + Schema, Guid));
                        return callback(null);
                    });
                }
            ], (err) => {
                if (err) {
                    console.log({ details: "RestoreData, snapShotTPData Error", error: err });
                    return callback(err);
                }
                return callback(null, Schema, Guid, Tag, Comment, Action, RollbackVersion, self);
            });
        } catch (error) {
            console.log({ details: "RestoreData,snapShotTPData Exception", error: error });
            return callback(error);
        }
    }
    rollbackData(Schema, Guid, Tag, Comment, Action, RollbackVersion, self, callback) {
        try {
            async.waterfall([
                (callback) => {
                    this.getConnection().then((redisClient) => redisClient.zrangebyscore("DataArchive:" + Schema + ":" + Guid, RollbackVersion, RollbackVersion, (err, res) => {
                        if (err) {
                            console.log({ details: "RestoreData zrangebyscore Error", error: err });
                            return callback(err);
                        } else if (res) {
                            var response = '';
                            try {
                                response = (typeof (res[0]) == "string") ? JSON.parse(res[0]) : res[0];
                            } catch (exp) {
                                console.log({ details: "RestoreData res parse : ", error: exp });
                                return callback(exp);
                            }
                            if (!response) {
                                return callback('No Backup Data Found to Rollback for the given Shortcode/GUID/Version combination.');
                            }
                            Reflect.deleteProperty(response, 'Tag');
                            Reflect.deleteProperty(response, 'Comment');
                            Reflect.deleteProperty(response, 'Action');
                            Reflect.deleteProperty(response, 'Version');
                            // delete response['Tag']; delete response['Comment']; delete response['Action']; delete response['Version'];
                            var items = [];
                            var keys = Object.keys(response);
                            for (var i = 0; i < keys.length; i++) {
                                var retObj = {};
                                var MetaData = response[keys[i]];
                                retObj.Schema = Schema;
                                retObj.Guid = Guid;
                                retObj.MetaData = MetaData;
                                items.push(retObj);
                            }
                            return callback(null, items);
                        }
                        return callback('Hash Key not found in Redis zrangebyscore');
                    }));
                },
                (DataItems, callback) => {
                    async.forEachOf(DataItems, (item, i, callback) => {
                        self.populateSearchIndex(item.Schema, item.Guid, item.MetaData, null, (err) => {
                            if (err) {
                                console.log({ details: "RestoreData forEachOf Error", error: err });
                            }
                            return callback(null);
                        });
                    }, (err) => {
                        if (err) {
                            console.log({ details: "RestoreData forEachOf Error", error: err });
                            return callback(err);
                        }
                        return callback(null);
                    });
                }
            ], (err) => {
                if (err) {
                    console.log({ details: "RestoreData forEachOf Error", error: err });
                    return callback(err);
                }
                return callback(null);
            });
        } catch (error) {
            console.log({ details: "RestoreData forEachOf Error", error: error });
            return callback(error);
        }
    }
    RestoreData(postData) {
        return new Promise((resolve, reject) => {
            var retJSON = {};
            try {
                var payload = this.getPayloadData(postData);

                if (!payload) {
                    return reject({ Status: false, Message: 'Invalid Postdata' });
                }

                var Schema = "", Guid = "", Tag = "", Comment = "", RollbackVersion = "", Action = "";
                var missingMandatoryKeys = "";
                if (payload.Schema) {
                    Schema = payload.Schema.toString();
                } else {
                    missingMandatoryKeys = "Schema";
                }
                if (payload.Guid) {
                    Guid = payload.Guid.toString();
                } else {
                    missingMandatoryKeys = (missingMandatoryKeys == "" ? "Guid" : missingMandatoryKeys + ", Guid");
                }
                if (payload.Version) {
                    RollbackVersion = payload.Version.toString();
                    Action = "Rollback - " + RollbackVersion;
                } else {
                    missingMandatoryKeys = (missingMandatoryKeys == "" ? "Version" : missingMandatoryKeys + ", Version");
                }

                if (missingMandatoryKeys != "") {
                    retJSON.Status = "false";
                    retJSON.Message = "missingMandatoryKeys : " + missingMandatoryKeys;
                    console.log({ details: "RestoreData missingMandatoryKeys", error: missingMandatoryKeys });
                    return resolve(retJSON);
                }
                if (this.RedisConnected) {
                    async.waterfall([
                        async.constant(Schema, Guid, Tag, Comment, Action, RollbackVersion, this),
                        conditional.if(this.checkTPData, this.snapShotData),
                        (Schema, Guid, Tag, Comment, Action, RollbackVersion, asyncself, callback) => {
                            this.rollbackData(Schema, Guid, Tag, Comment, Action, RollbackVersion, asyncself, callback);
                        }
                    ], (err) => {
                        if (err) {
                            console.log({ details: "RestoreData Error", error: err });
                            retJSON.Status = "false";
                            retJSON.Message = err;
                        } else {
                            retJSON.Status = "true";
                            retJSON.Message = "Success";
                        }
                        return resolve(retJSON);
                    });
                } else {
                    retJSON.Status = "false";
                    retJSON.Message = "Redis connection problem";
                    console.log({ details: "RestoreData ", error: "Redis connection problem" });
                    return resolve(retJSON);
                }
            } catch (err) {
                retJSON.Status = "false";
                retJSON.Message = err;
                console.log({ details: "RestoreData exception", error: err });
                return resolve(retJSON);
            }
        });
    }
    ListSearchIndex(postData) {
        return new Promise((resolve, reject) => {
            var retJSON = {};
            try {
                var payload = this.getPayloadData(postData);

                var Schema = "";
                var missingMandatoryKeys = "";

                if (payload.Schema) {
                    Schema = payload.Schema.toString();
                } else {
                    missingMandatoryKeys = (missingMandatoryKeys == "" ? "Schema" : missingMandatoryKeys + ", Schema");
                }
                if (missingMandatoryKeys != "") {
                    retJSON.Status = "false";
                    retJSON.Message = "missingMandatoryKeys : " + missingMandatoryKeys;
                    console.log({ details: "ListSearchIndex missingMandatoryKeys", error: missingMandatoryKeys });
                    return reject(retJSON);
                }

                const Hash = ["Index", Schema];
                this.getHash(Hash[0], Hash[1], (err, res) => {
                    if (err) {
                        retJSON.Status = "false";
                        retJSON.Message = err;
                        console.log("Error:ListSearchIndex", err);
                        return resolve(retJSON);
                    }
                    retJSON.Status = "true";
                    retJSON.Schema = Schema;
                    retJSON.SearchHash = JSON.parse(res);
                    return resolve(retJSON);
                });
            } catch (error) {
                console.log(error);
                retJSON.Status = 'false';
                retJSON.Error = error;
                console.log("Exception:ListSearchIndex", error);
                return resolve(retJSON);
            }
        });
    }
    RefreshSchemaSearchIndex(postData) {
        return new Promise((resolve, reject) => {
            var retJSON = {};
            try {
                var payload = this.getPayloadData(postData);

                if (!payload) {
                    return reject({ Status: false, Message: 'Invalid Postdata' });
                }

                var Schema = "";
                var missingMandatoryKeys = "";

                if (payload.Schema) {
                    Schema = payload.Schema.toString();
                } else {
                    missingMandatoryKeys = (missingMandatoryKeys == "" ? "Schema" : missingMandatoryKeys + ", Schema");
                }
                if (missingMandatoryKeys != "") {
                    retJSON.Status = "false";
                    retJSON.Message = "missingMandatoryKeys : " + missingMandatoryKeys;
                    console.log({ details: "RefreshSchemaSearchIndex missingMandatoryKeys", error: missingMandatoryKeys });
                    return resolve(retJSON);
                }
                if (this.RedisConnected) {
                    async.waterfall([
                        (callback) => {
                            this.getConnection().then((redisClient) => {
                                redisClient.smembers("Master:" + Schema, (err, res) => {
                                    if (err) {
                                        console.log({ details: "RefreshSchemaSearchIndex:getHashAll:Error", error: err });
                                        return callback(err);
                                    } else if (res) {
                                        return callback(null, res);
                                    }
                                    return callback('Hash Key not found in Redis');
                                });
                            });
                        },
                        (ScemaItems, callback) => {
                            async.forEachOfSeries(ScemaItems, (Guid, key, callbackeach) => {
                                this.getHashAll("Data:" + Schema + ":" + Guid, (err, res) => {
                                    if (err) {
                                        console.log({ details: "RefreshSchemaSearchIndex:getHashAll:Error", error: err });
                                        return callbackeach(null);
                                    } else if (res) {
                                        var items = [];
                                        var keys = Object.keys(res);
                                        for (var i = 0; i < keys.length; i++) {
                                            var retObj = {};
                                            var MetaData = res[keys[i]];
                                            retObj.Schema = Schema;
                                            retObj.Guid = Guid;
                                            retObj.MetaData = MetaData;
                                            items.push(retObj);
                                        }
                                        async.forEachOf(items, (item, i, callbackOf) => {
                                            this.populateSearchIndex(item.Schema, item.Guid, item.MetaData, 'Refresh', (err) => {
                                                if (err) {
                                                    console.log({ details: "RefreshSchemaSearchIndex:populateSearchIndex:Error", error: err });
                                                    return callbackOf(err);
                                                }
                                                callbackOf(null);
                                            });
                                        }, (err) => {
                                            if (err) {
                                                console.log({ details: "RefreshSchemaSearchIndex forEachOf exception", error: err });
                                                return callbackeach(err);
                                            }
                                            return callbackeach(null);
                                        });
                                    } else {
                                        return callbackeach(null);
                                    }
                                });
                            }, (err) => {
                                if (err) {
                                    console.log({ details: "RefreshSchemaSearchIndex:getHashAll:Error", error: err });
                                    return callback(err);
                                }
                                return callback(null);
                            });
                        }
                    ], (err) => {
                        if (err) {
                            console.log({ details: "RefreshSchemaSearchIndex waterfall", error: err });
                            retJSON.Status = "false";
                            retJSON.Message = err;
                        } else {
                            retJSON.Status = "true";
                            retJSON.Message = "Success";
                        }
                        return resolve(retJSON);
                    });
                } else {
                    console.log({ details: "RefreshSchemaSearchIndex", error: "Redis connection problem" });
                    retJSON.Status = "false";
                    retJSON.Message = "Redis connection problem";
                    return resolve(retJSON);
                }
            } catch (err) {
                console.log({ details: "RefreshSchemaSearchIndex exception", error: err });
                retJSON.Status = "false";
                retJSON.Message = err;
                return resolve(retJSON);
            }
        });
    }
    InsertData(postData) {
        return new Promise((resolve, reject) => {
            var retJSON = {};
            try {
                var payload = this.getPayloadData(postData);

                if (!payload.Schema) {
                    return reject({ Status: false, Message: 'Invalid Postdata' });
                }
                var newMetaData = "";
                // To do test encryption of metadata.
                if (payload.Certificate) {
                    var certificate = payload.Certificate;
                    try {
                        var certStr = ByteBuffer.atob(certificate.replace("BEGIN CERTIFICATE--- ", "").replace(" ---END CERTIFICATE", ""));
                        newMetaData = certStr;
                    } catch (err) {
                        console.log(err);
                        console.log({ details: "InsertData atob certificate exception", error: err });
                        return reject({ Status: false, Message: 'Invalid certificate Postdata' });
                    }
                }

                var Guid = "", Schema = "", MetaData = {}, Tag = '', Comment = '', Action = 'Update';
                var missingMandatoryKeys = "";
                if (payload.Guid) {
                    Guid = payload.Guid.toString().trim();
                } else {
                    Guid = null;
                }
                if (payload.Schema) {
                    Schema = payload.Schema.toString().trim();
                } else {
                    missingMandatoryKeys = (missingMandatoryKeys == "" ? "Schema" : missingMandatoryKeys + ", Schema");
                }
                if (payload.MetaData) {
                    MetaData = payload.MetaData.toString().trim();
                } else {
                    missingMandatoryKeys = (missingMandatoryKeys == "" ? "MetaData" : missingMandatoryKeys + ", MetaData");
                }

                if (newMetaData) {
                    console.log("Got A Certificate MetaData");
                    MetaData = newMetaData;
                    // Add Logic to replace MetaData with newMetaData if it exists
                }
                if (missingMandatoryKeys != "") {
                    retJSON.Status = "false";
                    retJSON.Message = "missingMandatoryKeys : " + missingMandatoryKeys;
                    console.log({ details: "InsertData missingMandatoryKeys", error: missingMandatoryKeys });
                    return resolve(retJSON);
                }
                this.dataInsert(Schema, uuid.v4(), MetaData, Tag, Comment, Action, retJSON, (err, result) => {
                    if (err) {
                        console.log(err);
                        return reject(err);
                    }
                    return resolve(result);
                });
            } catch (error) {
                console.log(error);
                retJSON.Status = "false";
                retJSON.Message = error;
                console.log({ details: "InsertData exception", error: error });
                return resolve(retJSON);
            }
        });
    }
    getAllSchemaData(Schema, Guid, items, callback) {
        // var retJSON = {};
        var finalResult = [];
        if (Guid) {
            this.getHash("Data:" + Schema, Guid, (err, retArr) => {
                if (err) {
                    console.log(err);
                    return callback(err);
                }
                if (items.length == 0) {
                    if (items[Schema]) {
                        items[Schema] = items[Schema].concat(retArr);
                    } else {
                        items[Schema] = items.concat(retArr);
                    }
                } else {
                    items[Schema] = items[Schema].concat(retArr);
                }
                return callback(null, result);
            });
        } else {
            this.getHashAll("Data:" + Schema, (err, retArr) => {
                if (err) {
                    console.log(err);
                    return callback(err);
                }
                if (items.length == 0) {
                    if (items[Schema]) {
                        items[Schema] = items[Schema].concat(retArr);
                    } else {
                        items[Schema] = items.concat(retArr);
                    }
                } else {
                    items[Schema] = items[Schema].concat(retArr);
                }
                return callback(null, result);
            })
        }
    }
    SearchMultipleData(postData) {
        return new Promise((resolve, reject) => {
            var retJSON = {};
            try {
                var payload = this.getPayloadData(postData);
                if (!payload) {
                    return reject({ Status: false, Message: 'Invalid Postdata' });
                }
                const Schema = Array.isArray(payload.Schema) ? payload.Schema : [payload.Schema];
                const Guid = payload.Guid ? payload.Guid : null;
                let PropertyFields = [];
                let PropertyValue = [];
                if (payload.PropertyField) {
                    PropertyFields = Array.isArray(payload.PropertyField) ? payload.PropertyField : [payload.PropertyField];
                } else {
                    PropertyFields = null;
                }
                if (payload.PropertyValue) {
                    PropertyValue = Array.isArray(payload.PropertyValue) ? payload.PropertyValue : [payload.PropertyValue];
                } else {
                    PropertyValue = null;
                }
                var newPayload = {
                    Schema: Schema
                };
                for (var PropertyField in PropertyFields) {
                    if (PropertyFields[PropertyField]) {
                        newPayload[PropertyFields[PropertyField]] = PropertyValue[PropertyField];
                    }
                }
                var forDeletion = ['Schema', 'Guid'];
                var arr = Object.keys(payload).filter((item) => !forDeletion.includes(item));
                var resultArray = [];
                if (arr.length > 0 && Guid === null) {
                    async.forEachOfSeries(Schema, (Shortcode, key, callbackeach) => {
                        this.SearchTPHash(Shortcode, newPayload, resultArray, callbackeach);
                    }, (err) => {
                        if (err) {
                            retJSON.Status = "false";
                            retJSON.Message = err;
                        } else {
                            retJSON.Status = "true";
                            retJSON.Message = "Success";
                            retJSON.items = Object.assign({}, resultArray);
                        }
                        return resolve(retJSON);
                    });
                } else if (arr.length === 0) {
                    async.forEachOfSeries(Schema, (Shortcode, key, callbackeach) => {
                        this.getAllSchemaData(Shortcode, Guid, resultArray, callbackeach);
                    }, (err) => {
                        if (err) {
                            retJSON.Status = "false";
                            retJSON.Message = err;
                        } else {
                            retJSON.Status = "true";
                            retJSON.Message = "Success";
                            retJSON.items = Object.assign({}, resultArray);
                        }
                        return resolve(retJSON);
                    });
                } else {
                    retJSON.Status = "false";
                    retJSON.Message = 'Please Change Search Post Data combination either (Guid only) or (' + arr.join(' and ') + ')';
                    return resolve(retJSON);
                }

            } catch (error) {
                retJSON.Status = "false";
                retJSON.Message = error;
                console.log(retJSON);
                return resolve(retJSON);
            }
        });
    }
    Search(postData) {
        return new Promise((resolve, reject) => {
            var retJSON = {};
            try {
                const recordFrom = postData.recordFrom ? postData.recordFrom : 0;
                const recordUpto = postData.recordUpto ? postData.recordUpto : 10;
                const payload = this.getPayloadData(postData);
                if (!payload) {
                    return reject({ Status: false, Message: 'Invalid Postdata' });
                }
                const Schema = Array.isArray(payload.Schema) ? payload.Schema : [payload.Schema];
                const Guid = payload.Guid ? payload.Guid : null;
                var forDeletion = ['Schema', 'Guid', 'recordFrom', 'recordUpto'];
                var arr = Object.keys(payload).filter((item) => !forDeletion.includes(item));
                var resultArray = [];
                if (arr.length > 0 && Guid === null) {
                    async.forEachOfSeries(Schema, (Shortcode, key, callbackeach) => {
                        this.SearchTPHash(Shortcode, payload, resultArray, callbackeach);
                    }, (err) => {
                        if (err) {
                            retJSON.Status = "false";
                            retJSON.Message = err;
                        } else {
                            retJSON.Status = "true";
                            retJSON.Message = "Success";
                            retJSON.TotalRecords = [];
                            for (const Shortcode of Schema) {
                                retJSON.TotalRecords = retJSON.TotalRecords.concat([Shortcode] + ':' + resultArray[Shortcode].length);
                                // retJSON[Shortcode + 'Count'] = resultArray[Shortcode].length;
                                resultArray[Shortcode] = resultArray[Shortcode].splice(recordFrom, recordUpto);
                            }
                            retJSON.items = Object.assign({}, resultArray);
                        }
                        return resolve(retJSON);
                    });
                } else {
                    async.forEachOfSeries(Schema, (Shortcode, key, callbackeach) => {
                        this.getAllSchemaData(Shortcode, Guid, resultArray, callbackeach);
                    }, (err) => {
                        if (err) {
                            retJSON.Status = "false";
                            retJSON.Message = err;
                        } else {
                            retJSON.Status = "true";
                            retJSON.Message = "Success";
                            retJSON.TotalRecords = [];
                            for (const Shortcode of Schema) {
                                retJSON.TotalRecords = retJSON.TotalRecords.concat([Shortcode] + ':' + resultArray[Shortcode].length);
                                resultArray[Shortcode] = resultArray[Shortcode].splice(recordFrom, recordUpto);
                            }
                            retJSON.items = Object.assign({}, resultArray);
                        }
                        return resolve(retJSON);
                    });
                }

            } catch (error) {
                retJSON.Status = "false";
                retJSON.Message = error;
                console.log(retJSON);
                return resolve(retJSON);
            }
        });
    }
    scanSearch(Search, finalResult, startFrom, endAt) {
        return new Promise((resolve, reject) => {
            this.getConnection().then((redisClient) => redisClient.scan(startFrom, 'MATCH', Search + '*', 'COUNT', endAt, (err, result) => {
                if (err) {
                    console.log('err:', err);
                    return reject(err);
                }
                for (const distinct of result[1]) {
                    finalResult = finalResult.concat(distinct.replace(Search, '')); // eslint-disable-line
                }
                if (result[0] == 0) {
                    return resolve(finalResult);
                }
                return resolve(this.scanSearch(Search, finalResult, result[0], endAt));
            }));
        });
    }
    getDistinctRecords(Payload) {
        return new Promise((resolve, reject) => {
            const RecordMoreThan = Payload.RecordMoreThan ? Payload.RecordMoreThan : 1000;
            const Search = `{SearchIndex}.${Payload.Schema}:${Payload.ShortCode}:`;
            const retJSON = {};
            let ResultArr = [];
            this.scanSearch(Search, ResultArr, 0, 10000).
                then((finalResult) => {
                    async.forEachOf(finalResult, (eachDistinct, key, callback) => {
                        this.getConnection().then((redisClient) => redisClient.scard(Search + eachDistinct, (err, count) => {
                            if (err) {
                                console.log(err);
                                return callback(err);
                            }
                            console.log(eachDistinct, count);
                            if (count >= RecordMoreThan) {
                                ResultArr = ResultArr.concat(eachDistinct);
                            }
                            return callback(null);
                        }));
                    }, (err) => {
                        if (err) {
                            console.log(err);
                            return reject(err);
                        }
                        retJSON.Status = 'Success';
                        retJSON.DistinctMatches = ResultArr.length;
                        retJSON.items = ResultArr;
                        return resolve(retJSON);
                    });
                }).
                catch((err) => {
                    console.log(err);
                    retJSON.Status = 'Failed';
                    retJSON.Message = err;
                    return reject(retJSON);
                });
        });
    }
    SearchTopRecords(postData) {
        const Schema = postData.Schema;
        const Nextbatch = postData.Nextbatch;
        const BATCHCOUNT = postData.BATCHCOUNT;
        const field = postData.field;
        let finalResult = [];
        return new Promise((resolve, reject) => {

            this.getConnection().then((redisClient) => redisClient.hscan("Data:" + Schema, Nextbatch, 'COUNT', BATCHCOUNT, (err, result) => {
                if (err) {
                    console.log('err:', err);
                    return reject(err);
                }
                let guidList = []
                for (const key in result[1]) {
                    if (key % 2 === 0) {
                        guidList = guidList.concat(result[1][key]);
                    }
                }
                async.forEachOf(guidList, (Guid, i, callback) => {
                    this.getHash("Data:" + Schema, Guid, (err, res) => {
                        if (err) {
                            console.log({ details: "getSchemaRecordsBatch:getHash:Error", error: err });
                        } else {
                            // console.log(JSON.parse(res));
                            try {
                                finalResult = finalResult.concat(JSON.parse(res)[field]); // eslint-disable-line
                            } catch (error) {
                                console.log(error);
                            }
                        }
                        return callback(null);
                    });
                }, (err) => {
                    if (err) {
                        console.log({ details: "GetAllSchemaData exception", error: err });
                        return reject(err);
                    }
                    if (result[0] == 0) {
                        return resolve([null, finalResult]);
                    }
                    return resolve([result[0], finalResult]);
                });
            }))

        });
    }
    Fetch(postData) {
        return new Promise((resolve, reject) => {
            // People Schema to get count people id
            let Search = 'Data:' + postData.Schema + ':';
            let schemaData = {};
            schemaData.Count = [];

            for (const joinedSchema of postData.JoinWith) {
                schemaData[joinedSchema] = [];
            }

            postData.JoinWith = postData.JoinWith.filter(e => e !== postData.Schema);

            this.getConnection().
                then((redisClient) => {
                    redisClient.hscan("Data:" + postData.Schema, postData.countStart, 'count', postData.countEnd, (err, res) => {
                        if (err) {
                            console.log(err);
                        }
                        console.log(res);
                        async.forEachOf(res[1], (Data, key, callback) => {
                            if (key % 2 === 0) {
                                return callback(null);
                            }
                            try {schemaData[postData.Schema].push(JSON.parse(Data));} catch (e) { console.log(e) };
                            async.forEachOf(postData.JoinWith, (joinSchema, i, callback) => {
                                this.getConnection().then((redisClient) => {
                                    redisClient.sscan("{SearchIndex}." + joinSchema + ':' + postData.Field + ':' + JSON.parse(Data)[postData.Field], 0, 'count', 1000, (err, setGuid) => {
                                        if (setGuid[1].length > 0) {
                                            const Hash = 'Data:' + joinSchema;
                                            this.getHash(Hash, setGuid[1], (err, result) => {
                                                if (result !== null) {
                                                    schemaData[joinSchema].push(JSON.parse(result));
                                                }
                                                return callback(null);
                                            });
                                        } else {
                                            return callback(null);
                                        }
                                    });
                                });
                            }, (err) => {
                                if (err) {
                                    console.log(err);
                                }
                                return callback(null);
                            });
                        }, (err) => {
                            if (err) {
                                console.log(err);
                                return reject(err);
                            }
                            for (const joinedSchema of postData.JoinWith) {
                                schemaData.Count = schemaData.Count.concat(joinedSchema + ':' + schemaData[joinedSchema].length);
                            }
                            schemaData.Count = schemaData.Count.concat(postData.Schema + ':' + schemaData[postData.Schema].length);
                            return resolve([res[0], schemaData]);
                        });
                    });
                });

        });

    }
    createConnection(Payload) {
        return new Promise((resolve, reject) => {
            console.error('use CreateConnection(), createConnection() is depricated');
            this.CreateConnection(Payload).
                then((result) => resolve(result)).
                catch((err) => reject(err));
        });
    }
    closeConnection(Payload) {
        return new Promise((resolve, reject) => {
            console.error('use CloseConnection(), closeConnection() is depricated');
            this.CloseConnection(Payload).
                then((result) => resolve(result)).
                catch((err) => reject(err));
        });
    }
    showStatus(Payload) {
        return new Promise((resolve, reject) => {
            console.error('use ShowConnectionStatus(), showStatus() is depricated');
            this.ShowConnectionStatus(Payload).
                then((result) => resolve(result)).
                catch((err) => reject(err));
        });
    }
    SetupSearchHash(Payload) {
        return new Promise((resolve, reject) => {
            console.error('use ConfigureSearchIndex(), SetupSearchHash() is depricated');
            this.ConfigureSearchIndex(Payload).
                then((result) => resolve(result)).
                catch((err) => reject(err));
        });
    }
    InsertTidalPoolSchema(Payload) {
        return new Promise((resolve, reject) => {
            console.error('use CreateSchema(), InsertTidalPoolSchema() is depricated');
            this.CreateSchema(Payload).
                then((result) => resolve(result)).
                catch((err) => reject(err));
        });
    }
    InsertTPData(Payload) {
        return new Promise((resolve, reject) => {
            console.error('use InsertData(), InsertTPData() is depricated');
            this.InsertData(Payload).
                then((result) => resolve(result)).
                catch((err) => reject(err));
        });
    }
    GetTidalPoolSchema(Payload) {
        return new Promise((resolve, reject) => {
            console.error('use GetAllSchemaData(), GetTidalPoolSchema() is depricated');
            this.GetAllSchemaData(Payload).
                then((result) => resolve(result)).
                catch((err) => reject(err));
        });
    }
    SearchTidalPoolHash(Payload) {
        return new Promise((resolve, reject) => {
            console.error('use SearchDataByProperty(), SearchTidalPoolHash() is depricated');
            this.SearchDataByProperty(Payload).
                then((result) => resolve(result)).
                catch((err) => reject(err));
        });
    }
    GetDatafromSchemas(Payload) {
        return new Promise((resolve, reject) => {
            console.error('use SearchMultipleData(), GetDatafromSchemas() is depricated');
            this.SearchMultipleData(Payload).
                then((result) => resolve(result)).
                catch((err) => reject(err));
        });
    }
    GetSchemaList(Payload) {
        return new Promise((resolve, reject) => {
            console.error('use ListSchemas(), GetSchemaList() is depricated');
            this.ListSchemas(Payload).
                then((result) => resolve(result)).
                catch((err) => reject(err));
        });
    }
    GetSearchHashSchema(Payload) {
        return new Promise((resolve, reject) => {
            console.error('use ListSearchIndex(), GetSearchHashSchema() is depricated');
            this.ListSearchIndex(Payload).
                then((result) => resolve(result)).
                catch((err) => reject(err));
        });
    }
    GetTpSearchHash(Payload) {
        return new Promise((resolve, reject) => {
            console.error('use ListSearchIndex(), GetTpSearchHash() is depricated');
            this.ListSearchIndex(Payload).
                then((result) => resolve(result)).
                catch((err) => reject(err));
        });
    }
    SetKeyData(Payload) {
        return new Promise((resolve, reject) => {
            console.error('use SetCacheData(), SetKeyData() is depricated');
            this.SetCacheData(Payload).
                then((result) => resolve(result)).
                catch((err) => reject(err));
        });
    }
    GetKeyData(Payload) {
        return new Promise((resolve, reject) => {
            console.error('use GetCacheData(), GetKeyData() is depricated');
            this.GetCacheData(Payload).
                then((result) => resolve(result)).
                catch((err) => reject(err));
        });
    }
    RefreshTidalPoolData(Payload) {
        return new Promise((resolve, reject) => {
            console.error('use RefreshSchemaSearchIndex(), RefreshTidalPoolData() is depricated');
            this.RefreshSchemaSearchIndex(Payload).
                then((result) => resolve(result)).
                catch((err) => reject(err));
        });
    }
    UpdateTidalPoolHash(Payload) {
        return new Promise((resolve, reject) => {
            console.error('use RefreshSchemaSearchIndex(), UpdateTidalPoolHash() is depricated');
            this.RefreshSchemaSearchIndex(Payload).
                then((result) => resolve(result)).
                catch((err) => reject(err));
        });
    }
    SnapshotTidalPoolData(Payload) {
        return new Promise((resolve, reject) => {
            console.error('use CreateBackupData(), SnapshotTidalPoolData() is depricated');
            this.CreateBackupData(Payload).
                then((result) => resolve(result)).
                catch((err) => reject(err));
        });
    }
    RemoveTidalPoolData(Payload) {
        return new Promise((resolve, reject) => {
            console.error('use RemoveData(), RemoveTidalPoolData() is depricated');
            this.RemoveData(Payload).
                then((result) => resolve(result)).
                catch((err) => reject(err));
        });
    }
    RollbackTidalPoolData(Payload) {
        return new Promise((resolve, reject) => {
            console.error('use RestoreData(), RollbackTidalPoolData() is depricated');
            this.RestoreData(Payload).
                then((result) => resolve(result)).
                catch((err) => reject(err));
        });
    }
};
