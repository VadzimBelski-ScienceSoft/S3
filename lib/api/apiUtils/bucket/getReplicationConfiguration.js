const parseXML = require('../../../utilities/parseXML');
const ReplicationConfiguration = require('./models/ReplicationConfiguration');

// Handle the steps for returning a valid replication configuration object.
function getReplicationConfiguration(xml, log, cb) {
    return parseXML(xml, log, (err, result) => {
        if (err) {
            return cb(err);
        }
        const validator = new ReplicationConfiguration(result).setLog(log);
        const configErr = validator.parseConfiguration();
        return cb(configErr || null, validator.getReplicationConfiguration());
    });
}

function getReplicationConfigurationXML(config, log) {
    const validator = new ReplicationConfiguration().setLog(log);
    return validator.getConfigXML(config);
}

module.exports = {
    getReplicationConfiguration,
    getReplicationConfigurationXML,
};
