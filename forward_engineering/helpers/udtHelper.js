module.exports = ({ _, commentIfDeactivated, assignTemplates, templates, getNamePrefixedWithSchemaName }) => {
    const getPlainUdt = (udt, columns) => {
        const udtName = getNamePrefixedWithSchemaName(udt.name, udt.databaseName);
        const comment = assignTemplates(templates.comment, {
            object: 'TYPE',
            objectName: udtName,
            comment: udt.comment,
        });

        switch (udt.type) {
            case 'composite':
                return assignTemplates(templates.createCompositeType, {
                    name: udtName,
                    columnDefinitions: _.join(columns, ',\n\t'),
                    comment: udt.comment ? comment : '',
                });
            case 'enum':
                return assignTemplates(templates.createEnumType, {
                    name: udtName,
                    values: _.map(udt.enum, value => `'${value}'`).join(', '),
                    comment: udt.comment ? comment : '',
                });
            case 'range_udt':
                return assignTemplates(templates.createRangeType, {
                    name: udtName,
                    subtype: udt.rangeSubtype,
                    options: getRangeOptions(udt),
                    comment: udt.comment ? comment : '',
                });
            default:
                return '';
        }
    };

    const getRangeOptions = udt => {
        const wrap = value => (value ? `\t${value}` : '');

        const statements = [
            { key: 'operatorClass', getValue: getBasicValue('SUBTYPE_OPCLASS') },
            { key: 'collation', getValue: getBasicValue('COLLATION') },
            { key: 'canonicalFunction', getValue: getBasicValue('CANONICAL') },
            { key: 'subtypeDiffFunction', getValue: getBasicValue('SUBTYPE_DIFF') },
            { key: 'multiRangeType', getValue: getBasicValue('MULTIRANGE_TYPE_NAME') },
        ]
            .map(config => wrap(config.getValue(udt[config.key])))
            .filter(Boolean)
            .join(',\n');

        return _.trim(statements) ? ',\n\t' + _.trim(statements) : '';
    };

    const getBasicValue = prefix => value => {
        if (value) {
            return `${prefix}=${value}`;
        }
    };

    const getUserDefinedType = (udt, columns) => {
        return commentIfDeactivated(getPlainUdt(udt, columns), {
            isActivated: udt.isActivated,
        });
    };

    return { getUserDefinedType };
};