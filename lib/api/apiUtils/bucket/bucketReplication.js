import { ReplicationConfiguration } from './models/ReplicationConfiguration';

// Handle the steps for returning a valid replication configuration object.
export function getReplicationConfiguration(xml, log, cb) {
    const validator = new ReplicationConfiguration(xml, log);
    return validator.parseXML(err => {
        if (err) {
            return cb(err);
        }
        const configErr = validator.buildConfigurationObject() || null;
        return cb(configErr, validator.replicationConfiguration);
    });
}
