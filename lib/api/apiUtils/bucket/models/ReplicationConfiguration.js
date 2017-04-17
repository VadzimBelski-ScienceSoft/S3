import assert from 'assert';

import { errors } from 'arsenal';
import { parseString } from 'xml2js';

/**
    Example XML request:

    <ReplicationConfiguration>
        <Role>IAM-role-ARN</Role>
        <Rule>
            <ID>Rule-1</ID>
            <Status>rule-status</Status>
            <Prefix>key-prefix</Prefix>
            <Destination>
                <Bucket>arn:aws:s3:::bucket-name</Bucket>
                <StorageClass>
                    optional-destination-storage-class-override
                </StorageClass>
            </Destination>
        </Rule>
        <Rule>
            <ID>Rule-2</ID>
            ...
        </Rule>
        ...
    </ReplicationConfiguration>
*/

export class ReplicationConfiguration {
    /**
     * Create a ReplicationConfiguration instance
     * @param {string} xml - The XML string to be parsed
     * @param {object} log - Werelogs logger
     * @return {object} - ReplicationConfiguration instance
     */
    constructor(xml, log) {
        this._xml = xml;
        this._log = log;
        this._parsedXML = null;
        this._destinationBucket = null;
        this._configPrefixes = [];
        this._configIDs = [];
        this.replicationConfiguration = {
            role: null,
            destination: null, // The bucket to replicate data to.
            rules: [
                {
                    storageClass: null,
                    prefix: null,
                    status: null,
                    id: null,
                },
            ],
        };
    }

    /**
     * Handle the initial parsing of XML using the `parseString` method
     * @param {function} cb - Callback to call
     * @return {undefined}
     */
    parseXML(cb) {
        if (this._xml === '') {
            this._log.debug('request xml is missing');
            return cb(errors.MalformedXML);
        }
        return parseString(this._xml, (err, result) => {
            if (err) {
                this._log.debug('request xml is malformed');
                return cb(errors.MalformedXML);
            }
            this._parsedXML = result;
            return cb();
        });
    }

    /**
     * Build the rule object from the parsed XML of the given rule
     * @param {object} rule - The rule object from this._parsedXML
     * @return {object} - The rule object to push into the `Rules` array
     */
    _getRuleObject(rule) {
        const obj = {
            prefix: rule.Prefix[0],
            status: rule.Status[0],
        };
        // StorageClass is an optional property.
        if (rule.Destination[0].StorageClass) {
            obj.storageClass = rule.Destination[0].StorageClass[0];
        }
        // ID is an optional property.
        if (rule.ID) {
            obj.id = rule.ID[0];
        }
        return obj;
    }

    /**
     * Check that the `Role` property of the configuration is valid
     * @return {undefined}
     */
    _checkRole() {
        const Role = this._parsedXML.ReplicationConfiguration.Role;
        if (!Role) {
            return errors.MalformedXML;
        }
        // TODO: Update to validate role priveleges after implemented in Vault.
        // Role should be an IAM user name.
        const arr = Role[0].split(':');
        const isValidARN = arr.length === 7 ||
            (arr.length === 6 && arr[5].split('/').length === 2);
        if (!isValidARN) {
            return errors.InvalidArgument.customizeDescription(
                'Invalid Role specified in replication configuration');
        }
        this.replicationConfiguration.role = Role[0];
        return undefined;
    }

    /**
     * Check that the `Rules` property array is valid
     * @return {undefined}
     */
    _checkRules() {
        // Note that the XML uses 'Rule' while the config object uses 'Rules'.
        const { Rule } = this._parsedXML.ReplicationConfiguration;
        if (!Rule || Rule.length < 1) {
            return errors.MalformedXML;
        }
        if (Rule.length > 1000) {
            return errors.InvalidRequest.customizeDescription(
                'Number of defined replication rules cannot exceed 1000');
        }
        const err = this._checkEachRule(Rule);
        if (err) {
            return err;
        }
        this.replicationConfiguration.rules = this._rules;
        return undefined;
    }

    /**
     * Check that each rule in the `Rules` property array is valid
     * @param {array} rules - The rule array from this._parsedXML
     * @return {undefined}
     */
    _checkEachRule(rules) {
        const rulesArr = [];
        for (let i = 0; i < rules.length; i++) {
            const err =
                this._checkStatus(rules[i]) || this._checkPrefix(rules[i]) ||
                this._checkID(rules[i]) || this._checkDestination(rules[i]);
            if (err) {
                return err;
            }
            rulesArr.push(this._getRuleObject(rules[i]));
        }
        this._rules = rulesArr;
        return undefined;
    }

    /**
     * Check that the `Status` property is valid
     * @param {object} rule - The rule object from this._parsedXML
     * @return {undefined}
     */
    _checkStatus(rule) {
        const status = rule.Status && rule.Status[0];
        if (!status || !['Enabled', 'Disabled'].includes(status)) {
            return errors.MalformedXML;
        }
        return undefined;
    }

    /**
     * Check that the `Prefix` property is valid
     * @param {object} rule - The rule object from this._parsedXML
     * @return {undefined}
     */
    _checkPrefix(rule) {
        const prefix = rule.Prefix && (rule.Prefix[0] || rule.Prefix[0] === '');
        if (!prefix && prefix !== '') {
            return errors.MalformedXML;
        }
        if (prefix.length > 1024) {
            return errors.InvalidArgument.customizeDescription('Rule prefix ' +
                'cannot be longer than maximum allowed key length of 1024');
        }
        // Each Prefix in a list of rules must be unique.
        if (this._configPrefixes.includes(prefix)) {
            return errors.InvalidRequest.customizeDescription(
                `Found overlapping prefixes '${prefix}' and '${prefix}'`);
        }
        this._configPrefixes.push(prefix);
        return undefined;
    }

    /**
     * Check that the `ID` property is valid
     * @param {object} rule - The rule object from this._parsedXML
     * @return {undefined}
     */
    _checkID(rule) {
        const id = rule.ID && rule.ID[0];
        if (id && id.length > 255) {
            return errors.InvalidArgument
                .customizeDescription('Rule Id cannot be greater than 255');
        }
        // Each ID in a list of rules must be unique.
        if (this._configIDs.includes(id)) {
            return errors.InvalidRequest.customizeDescription(
                'Rule Id must be unique');
        }
        this._configIDs.push(id);
        return undefined;
    }

    /**
     * Check that the `StorageClass` property is valid
     * @param {object} destination - The destination object from this._parsedXML
     * @return {undefined}
     */
    _checkStorageClass(destination) {
        const storageClass = destination.StorageClass &&
            destination.StorageClass[0];
        const validStorageClasses = [
            'STANDARD',
            'STANDARD_IA',
            'REDUCED_REDUNDANCY',
        ];
        if (storageClass && !validStorageClasses.includes(storageClass)) {
            return errors.MalformedXML;
        }
        return undefined;
    }

    /**
     * Check that the `Bucket` property is valid
     * @param {object} destination - The destination object from this._parsedXML
     * @return {undefined}
     */
    _checkBucket(destination) {
        const bucket = destination.Bucket && destination.Bucket[0];
        if (!bucket) {
            return errors.MalformedXML;
        }
        const isValidARN = bucket.split(':').length === 6;
        if (!isValidARN) {
            return errors.InvalidArgument
                .customizeDescription('Invalid bucket ARN');
        }
        // We can replicate objects only to one destination bucket.
        if (this.replicationConfiguration.destination &&
            this.replicationConfiguration.destination !== bucket) {
            return errors.InvalidRequest.customizeDescription(
                'The destination bucket must be same for all rules');
        }
        this.replicationConfiguration.destination = bucket;
        return undefined;
    }

    /**
     * Check that the `destination` property is valid
     * @param {object} rule - The rule object from this._parsedXML
     * @return {undefined}
     */
    _checkDestination(rule) {
        const dest = rule.Destination && rule.Destination[0];
        if (!dest) {
            return errors.MalformedXML;
        }
        let err = this._checkBucket(dest);
        if (err) {
            return err;
        }
        err = this._checkStorageClass(dest);
        if (err) {
            return err;
        }
        return undefined;
    }

    /**
     * Check that the request configuration is valid
     * @return {undefined}
     */
    buildConfigurationObject() {
        let err = this._checkRole();
        if (err) {
            return err;
        }
        err = this._checkRules();
        if (err) {
            return err;
        }
        return undefined;
    }

    /**
     * Validate the expected configuration properties
     * @param {object} config - The replication configuration to validate
     * @return {undefined}
     */
    static validateConfig(config) {
        assert.strictEqual(typeof config, 'object');
        const { role, rules, destination } = config;
        assert.strictEqual(typeof role, 'string');
        assert.strictEqual(typeof destination, 'string');
        assert.strictEqual(Array.isArray(rules), true);
        rules.forEach(rule => {
            assert.strictEqual(typeof rule, 'object');
            const { prefix, status, id, storageClass } = rule;
            assert.strictEqual(typeof prefix, 'string');
            assert.strictEqual(typeof status, 'string');
            assert(status === 'Enabled' || status === 'Disabled');
            assert(id === undefined || typeof id === 'string');
            assert(storageClass === undefined ||
                storageClass === 'STANDARD' ||
                storageClass === 'REDUCED_REDUNDANCY' ||
                storageClass === 'STANDARD_IA');
        });
    }
}
