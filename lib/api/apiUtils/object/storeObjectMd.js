const { errors } = require('arsenal');

/** validateUserMetadata - check user-provided metadata is not too large
 * @param {objec} metaHeaders - object containing user-provided metaheaders
 * @return {(Error|null)} - return MetadataTooLarge or null
 */
function validateUserMetadata(metaHeaders) {
    let totalLength = 0;
    Object.keys(metaHeaders).forEach(key => {
        totalLength += key.length;
        totalLength += metaHeaders[key].length;
    });
    if (totalLength > 2136) {
        return errors.MetadataTooLarge;
    }
    return null;
}

module.exports = {
    validateUserMetadata,
};
