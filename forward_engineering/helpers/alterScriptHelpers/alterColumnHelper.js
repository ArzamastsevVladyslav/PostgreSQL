const { checkFieldPropertiesChanged } = require('./common');
const assignTemplates = require("../../utils/assignTemplates");
const templates = require("../../configs/templates");

const getUpsertCommentOnColumnScripts = (_, collection) => {
    const {wrapComment, wrapInQuotes} = require("../general")({_});
    const out = [];
    for (const key of Object.keys(collection.properties)) {
        const newComment = collection.properties[key].description;
        const oldComment = collection.role.properties[key]?.description;
        if (newComment) {
            if (!oldComment || newComment !== oldComment) {
                const script = assignTemplates(templates.comment, {
                    object: 'COLUMN',
                    objectName: wrapInQuotes(key),
                    comment: wrapComment(newComment),
                });
                out.push(script);
            }
        }
    }
    return out;
}

const getDeleteCommentOnColumnScripts = (_, collection) => {
    const {wrapInQuotes} = require("../general")({_});
    const out = [];
    for (const key of Object.keys(collection.role.properties)) {
        const newComment = collection.properties[key]?.description;
        const oldComment = collection.role.properties[key].description;
        if (oldComment && !newComment) {
            const script = assignTemplates(templates.dropComment, {
                object: 'COLUMN',
                objectName: wrapInQuotes(key),
            });
            out.push(script);
        }
    }
    return out;
}

const getFullTableName = (_, collection) => {
    const { getNamePrefixedWithSchemaName } = require('../general')({ _ });
    const { getEntityName } = require('../../utils/general')(_);
    const collectionSchema = { ...collection, ...(_.omit(collection?.role, 'properties') || {}) };
    const tableName = getEntityName(collectionSchema);
    const schemaName = collectionSchema.compMod?.keyspaceName;
    return getNamePrefixedWithSchemaName(tableName, schemaName);
}

const getRenameColumnScripts = (_, collection) => {
    const { wrapInQuotes } = require('../general')({ _ });
    const fullName = getFullTableName(_, collection);

    return _.values(collection.properties)
        .filter(jsonSchema => checkFieldPropertiesChanged(jsonSchema.compMod, ['name']))
        .map(
            jsonSchema =>
                `ALTER TABLE IF EXISTS ${fullName} RENAME COLUMN ${wrapInQuotes(
                    jsonSchema.compMod.oldField.name,
                )} TO ${wrapInQuotes(jsonSchema.compMod.newField.name)};`,
        );
}

 const getChangeColumnTypeScripts = (_, collection) => {
    const { wrapInQuotes } = require('../general')({ _ });
    const fullName = getFullTableName(_, collection);

    return _.toPairs(collection.properties)
        .filter(([name, jsonSchema]) => checkFieldPropertiesChanged(jsonSchema.compMod, ['type', 'mode']))
        .map(
            ([name, jsonSchema]) =>
                `ALTER TABLE IF EXISTS ${fullName} ALTER COLUMN ${wrapInQuotes(name)} SET DATA TYPE ${
                        jsonSchema.compMod.newField.mode || jsonSchema.compMod.newField.type
                };`,
        );
}

module.exports = {
    getChangeColumnTypeScripts,
    getRenameColumnScripts,
    getUpsertCommentOnColumnScripts,
    getDeleteCommentOnColumnScripts
}
