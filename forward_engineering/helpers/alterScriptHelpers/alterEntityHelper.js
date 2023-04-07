const {
    getUpsertCommentOnColumnScripts,
    getDeleteCommentOnColumnScripts,
    getRenameColumnScripts,
    getChangeColumnTypeScripts
} = require("./alterColumnHelper");

const getAddCollectionScript =
    ({app, dbVersion, modelDefinitions, internalDefinitions, externalDefinitions}) =>
        collection => {
            const _ = app.require('lodash');
            const {getEntityName} = require('../../utils/general')(_);
            const {createColumnDefinitionBySchema} = require('./createColumnDefinition')(app);
            const ddlProvider = require('../../ddlProvider')(null, null, app);
            const {getDefinitionByReference} = app.require('@hackolade/ddl-fe-utils');

            const schemaName = collection.compMod.keyspaceName;
            const schemaData = {schemaName, dbVersion};
            const jsonSchema = {...collection, ...(_.omit(collection?.role, 'properties') || {})};
            const columnDefinitions = _.toPairs(jsonSchema.properties).map(([name, column]) => {
                const definitionJsonSchema = getDefinitionByReference({
                    propertySchema: column,
                    modelDefinitions,
                    internalDefinitions,
                    externalDefinitions,
                });

                return createColumnDefinitionBySchema({
                    name,
                    jsonSchema: column,
                    parentJsonSchema: jsonSchema,
                    ddlProvider,
                    schemaData,
                    definitionJsonSchema,
                });
            });
            const checkConstraints = (jsonSchema.chkConstr || []).map(check =>
                ddlProvider.createCheckConstraint(ddlProvider.hydrateCheckConstraint(check)),
            );
            const tableData = {
                name: getEntityName(jsonSchema),
                columns: columnDefinitions.map(ddlProvider.convertColumnDefinition),
                checkConstraints: checkConstraints,
                foreignKeyConstraints: [],
                schemaData,
                columnDefinitions,
                dbData: {dbVersion},
            };
            const hydratedTable = ddlProvider.hydrateTable({tableData, entityData: [jsonSchema], jsonSchema});

            return ddlProvider.createTable(hydratedTable, jsonSchema.isActivated);
        };

const getDeleteCollectionScript = app => collection => {
    const _ = app.require('lodash');
    const {getEntityName} = require('../../utils/general')(_);
    const {getNamePrefixedWithSchemaName} = require('../general')({_});

    const jsonData = {...collection, ...(_.omit(collection?.role, 'properties') || {})};
    const tableName = getEntityName(jsonData);
    const schemaName = collection.compMod.keyspaceName;
    const fullName = getNamePrefixedWithSchemaName(tableName, schemaName);

    return `DROP TABLE IF EXISTS ${fullName};`;
};

const getAddColumnScript =
    ({app, dbVersion, modelDefinitions, internalDefinitions, externalDefinitions}) =>
        collection => {
            const _ = app.require('lodash');
            const {getEntityName} = require('../../utils/general')(_);
            const {getNamePrefixedWithSchemaName} = require('../general')({_});
            const {createColumnDefinitionBySchema} = require('./createColumnDefinition')(app);
            const ddlProvider = require('../../ddlProvider')(null, null, app);
            const {getDefinitionByReference} = app.require('@hackolade/ddl-fe-utils');

            const collectionSchema = {...collection, ...(_.omit(collection?.role, 'properties') || {})};
            const tableName = getEntityName(collectionSchema);
            const schemaName = collectionSchema.compMod?.keyspaceName;
            const fullName = getNamePrefixedWithSchemaName(tableName, schemaName);
            const schemaData = {schemaName, dbVersion};

            return _.toPairs(collection.properties)
                .filter(([name, jsonSchema]) => !jsonSchema.compMod)
                .map(([name, jsonSchema]) => {
                    const definitionJsonSchema = getDefinitionByReference({
                        propertySchema: jsonSchema,
                        modelDefinitions,
                        internalDefinitions,
                        externalDefinitions,
                    });

                    return createColumnDefinitionBySchema({
                        name,
                        jsonSchema,
                        parentJsonSchema: collectionSchema,
                        ddlProvider,
                        schemaData,
                        definitionJsonSchema,
                    });
                })
                .map(ddlProvider.convertColumnDefinition)
                .map(script => `ALTER TABLE IF EXISTS ${fullName} ADD COLUMN IF NOT EXISTS ${script};`);
        };

const getDeleteColumnScript = app => collection => {
    const _ = app.require('lodash');
    const {getEntityName} = require('../../utils/general')(_);
    const {getNamePrefixedWithSchemaName, wrapInQuotes} = require('../general')({_});

    const collectionSchema = {...collection, ...(_.omit(collection?.role, 'properties') || {})};
    const tableName = getEntityName(collectionSchema);
    const schemaName = collectionSchema.compMod?.keyspaceName;
    const fullName = getNamePrefixedWithSchemaName(tableName, schemaName);

    return _.toPairs(collection.properties)
        .filter(([name, jsonSchema]) => !jsonSchema.compMod)
        .map(([name]) => `ALTER TABLE IF EXISTS ${fullName} DROP COLUMN IF EXISTS ${wrapInQuotes(name)};`);
};

const getModifyColumnScript = app => collection => {
    const _ = app.require('lodash');

    const renameColumnScripts = getRenameColumnScripts(_, collection)
    const changeTypeScripts = getChangeColumnTypeScripts(_, collection)
    const upsertColumnCommentScripts = getUpsertCommentOnColumnScripts(_, collection);
    const deleteColumnCommentScripts = getDeleteCommentOnColumnScripts(_, collection);

    return [...renameColumnScripts, ...changeTypeScripts, ...upsertColumnCommentScripts, ...deleteColumnCommentScripts];
};

module.exports = {
    getAddCollectionScript,
    getDeleteCollectionScript,
    getAddColumnScript,
    getDeleteColumnScript,
    getModifyColumnScript,
};
