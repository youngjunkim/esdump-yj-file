var util = require("util");
var http = require("http");
var https = require("https");
var EventEmitter = require('events').EventEmitter;
var isUrl = require('./lib/is-url');

var elasticdump = function(input, output, options) {
    var self = this;

    self.input = input;
    self.output = output;
    self.options = options;

    if (!self.options.searchBody) {
        self.options.searchBody = {
            "query": {
                "match_all": {}
            },
            "fields": ["*"],
            "_source": true
        };
    }

    if (self.options.toLog === null || self.options.toLog === undefined) {
        self.options.toLog = true;
    }

    self.validationErrors = self.validateOptions();

    if (options.maxSockets) {
        self.log('globally setting maxSockets=' + options.maxSockets);
        http.globalAgent.maxSockets = options.maxSockets;
        https.globalAgent.maxSockets = options.maxSockets;
    }

    var InputProto;
    if (self.options.input && !self.options.inputTransport) {
        if (self.options.input === "$") {
            self.inputType = 'stdio';
        } else if (isUrl(self.options.input)) {
            self.inputType = 'elasticsearch';
        } else {
            self.inputType = 'file';
        }

        InputProto = require(__dirname + "/lib/transports/" + self.inputType)[self.inputType];
        self.input = (new InputProto(self, self.options.input, self.options['input-index']));
    } else if (self.options.inputTransport) {
        InputProto = require(self.options.inputTransport);
        var inputProtoKeys = Object.keys(InputProto);
        self.input = (new InputProto[inputProtoKeys[0]](self, self.options.input, self.options['input-index']));
    }

    var OutputProto;
    if (self.options.output && !self.options.outputTransport) {
        if (self.options.output === "$") {
            self.outputType = 'stdio';
            self.options.toLog = false;
        } else if (isUrl(self.options.output)) {
            self.outputType = 'elasticsearch';
        } else {
            self.outputType = 'file';
        }

        OutputProto = require(__dirname + "/lib/transports/" + self.outputType)[self.outputType];
        self.output = (new OutputProto(self, self.options.output, self.options['output-index']));
    } else if (self.options.outputTransport) {
        OutputProto = require(self.options.outputTransport);
        var outputProtoKeys = Object.keys(OutputProto);
        self.output = (new OutputProto[outputProtoKeys[0]](self, self.options.output, self.options['output-index']));
    }
};

util.inherits(elasticdump, EventEmitter);

elasticdump.prototype.log = function(message) {
    var self = this;

    if (typeof self.options.logger === 'function') {
        self.options.logger(message);
    } else if (self.options.toLog === true) {
        self.emit("log", message);
    }
};

elasticdump.prototype.validateOptions = function() {
    var self = this;
    var validationErrors = [];

    var required = ['input', 'output'];
    required.forEach(function(v) {
        if (!self.options[v]) {
            validationErrors.push('`' + v + '` is a required input');
        }
    });

    return validationErrors;
};

elasticdump.prototype.dump = function(callback, continuing, limit, offset, total_writes) {
    var self = this;

    if (self.validationErrors.length > 0) {
        self.emit('error', {
            errors: self.validationErrors
        });
        callback(new Error('There was an error starting this dump'));
    } else {

        if (!limit) {
            limit = self.options.limit;
        }
        if (!offset) {
            offset = self.options.offset;
        }
        if (!total_writes) {
            total_writes = 0;
        }

        if (continuing !== true) {
            self.log('starting dump');

            if (self.options.skip) {
                self.log('Warning: skipping ' + self.options.skip + ' rows.');
                self.log("  * skipping doesn't guarantee that the skipped rows have already been written, please refer to the README.");
            }
        }

        self.input.get(limit, offset, function(err, data) {
            if (err) {
                self.emit('error', err);
            }
            if (!err || (self.options['ignore-errors'] === true || self.options['ignore-errors'] === 'true')) {
                self.log("got " + data.length + " objects from source " + self.inputType + " (offset: " + offset + ")");

                var data01 = [];
                for (var i = 0; i < data.length; i++) {

                    if (data[i]._source.title && data[i]._source.words) {
                        
                        //for source_type grants_* 
                        if (data[i]._source.meta.award_floor) {
                            if (data[i]._source.meta.award_floor != 'none') {
                                data[i]._source.meta.award_floor = (typeof data[i]._source.meta.award_floor != "number") ? parseInt(data[i]._source.meta.award_floor) : data[i]._source.meta.award_floor;
                            } else {
                                delete data[i]._source.meta.award_floor;
                            }
                        }

                        if (data[i]._source.meta.award_ceiling) {
                            if (data[i]._source.meta.award_ceiling != 'none') {
                                data[i]._source.meta.award_ceiling = (typeof data[i]._source.meta.award_ceiling != "number") ? parseInt(data[i]._source.meta.award_ceiling) : data[i]._source.meta.award_ceiling;
                            } else {
                                delete data[i]._source.meta.award_ceiling;
                            }
                        }

                        if (data[i]._source.meta.estimated_funding) {
                            if (data[i]._source.meta.estimated_funding != 'none') {
                                data[i]._source.meta.estimated_funding = (typeof data[i]._source.meta.estimated_funding != "number") ? parseInt(data[i]._source.meta.estimated_funding) : data[i]._source.meta.estimated_funding;
                            } else {
                                delete data[i]._source.meta.estimated_funding;
                            }
                        }

                        delete data[i]._source.top_words;
                        data01.push(data[i]);
                    }
                }

                self.output.set(data01, limit, offset, function(err, writes) {

                    var toContinue = true;
                    if (err) {
                        self.emit('error', err);
                        if (self.options['ignore-errors'] === true || self.options['ignore-errors'] === 'true') {
                            toContinue = true;
                        } else {
                            toContinue = false;
                        }
                    } else {
                        total_writes += writes;
                        self.log("sent " + data01.length + " objects to destination " + self.outputType + ", wrote " + writes);
                        offset = offset + data01.length;
                    }
                    if (data01.length > 0 && toContinue) {
                        self.dump(callback, true, limit, offset, total_writes);
                    } else if (toContinue) {
                        self.log('Total Writes: ' + total_writes);
                        self.log('dump complete');
                        if (typeof callback === 'function') {
                            callback(null, total_writes);
                        }
                    } else if (toContinue === false) {
                        self.log('Total Writes: ' + total_writes);
                        self.log('dump ended with error (set phase)  => ' + String(err));
                        if (typeof callback === 'function') {
                            callback(err, total_writes);
                        }
                    }
                });
            } else {
                self.log('Total Writes: ' + total_writes);
                self.log('dump ended with error (get phase) => ' + String(err));
                if (typeof callback === 'function') {
                    callback(err, total_writes);
                }
            }
        });
    }
};

exports.elasticdump = elasticdump;