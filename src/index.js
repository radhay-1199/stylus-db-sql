const parseQuery = require('./queryParser');
const readCSV = require('./csvReader');

async function executeSELECTQuery(query) {
    const { fields, table, whereClauses, joinType, joinTable, joinCondition, groupByFields } = parseQuery(query);
    let data = await readCSV(`${table}.csv`);

    // Logic for applying JOINs
    if (joinTable && joinCondition) {
        const joinData = await readCSV(`${joinTable}.csv`);
        switch (joinType.toUpperCase()) {
            case 'INNER':
                data = performInnerJoin(data, joinData, joinCondition, fields, table);
                break;
            case 'LEFT':
                data = performLeftJoin(data, joinData, joinCondition, fields, table);
                break;
            case 'RIGHT':
                data = performRightJoin(data, joinData, joinCondition, fields, table);
                break;
            // Optional: handle default case or unsupported JOIN types
        }
    }

    // Apply WHERE clause filtering after JOIN (or on the original data if no join)
    data = whereClauses.length > 0
        ? data.filter(row => whereClauses.every(clause => evaluateCondition(row, clause)))
        : data;

    // Group by functionality
    if (groupByFields) {
        const groupedData = applyGroupBy(data, groupByFields, fields);

        // Flattening the grouped data into an array
        data = Object.values(groupedData).flat();
    }

    // Select the specified fields
    return data.map(row => {
        const selectedRow = {};
        fields.forEach(field => {
            selectedRow[field] = row[field];
        });
        return selectedRow;
    });
}

function evaluateCondition(row, clause) {
    const { field, operator, value } = clause;
    switch (operator) {
        case '=': return row[field] === value;
        case '!=': return row[field] !== value;
        case '>': return row[field] > value;
        case '<': return row[field] < value;
        case '>=': return row[field] >= value;
        case '<=': return row[field] <= value;
        default: throw new Error(`Unsupported operator: ${operator}`);
    }
}

function performInnerJoin(mainData, joinData, joinCondition, fields, mainTable) {
    // Logic for INNER JOIN
    return mainData.flatMap(mainRow => {
        return joinData
            .filter(joinRow => {
                const mainValue = mainRow[joinCondition.left.split('.')[1]];
                const joinValue = joinRow[joinCondition.right.split('.')[1]];
                return mainValue === joinValue;
            })
            .map(joinRow => {
                return fields.reduce((acc, field) => {
                    const [tableName, fieldName] = field.split('.');
                    acc[field] = tableName === mainTable ? mainRow[fieldName] : joinRow[fieldName];
                    return acc;
                }, {});
            });
    });
}

function performLeftJoin(mainData, joinData, joinCondition, fields, mainTable) {
    return mainData.map(mainRow => {
        const joinRows = joinData.filter(joinRow => {
            const mainValue = mainRow[joinCondition.left.split('.')[1]];
            const joinValue = joinRow[joinCondition.right.split('.')[1]];
            return mainValue === joinValue;
        });

        if (joinRows.length === 0) {
            return fields.reduce((acc, field) => {
                const [tableName, fieldName] = field.split('.');
                acc[field] = tableName === mainTable ? mainRow[fieldName] : null;
                return acc;
            }, {});
        }

        return joinRows.map(joinRow => {
            return fields.reduce((acc, field) => {
                const [tableName, fieldName] = field.split('.');
                acc[field] = tableName === mainTable ? mainRow[fieldName] : joinRow[fieldName];
                return acc;
            }, {});
        });
    }).flat();
}

function performRightJoin(mainData, joinData, joinCondition, fields, mainTable) {
    return joinData.map(joinRow => {
        const mainRows = mainData.filter(mainRow => {
            const mainValue = mainRow[joinCondition.left.split('.')[1]];
            const joinValue = joinRow[joinCondition.right.split('.')[1]];
            return mainValue === joinValue;
        });

        if (mainRows.length === 0) {
            return fields.reduce((acc, field) => {
                const [tableName, fieldName] = field.split('.');
                acc[field] = tableName === mainTable ? null : joinRow[fieldName];
                return acc;
            }, {});
        }

        return mainRows.map(mainRow => {
            return fields.reduce((acc, field) => {
                const [tableName, fieldName] = field.split('.');
                acc[field] = tableName === mainTable ? mainRow[fieldName] : joinRow[fieldName];
                return acc;
            }, {});
        });
    }).flat();
}

function applyGroupBy(data, groupByFields, fields) {
    const groupedData = data.reduce((acc, row) => {
        const key = groupByFields.map(field => row[field]).join('|');
        acc[key] = acc[key] || [];
        acc[key].push(row);
        return acc;
    }, {});

    return Object.values(groupedData).map(group => {
        const result = {};
        groupByFields.forEach(field => {
            result[field] = group[0][field]; // Populate group by fields
        });

        fields.forEach(field => {
            if (isAggregateField(field)) {
                const { func, col } = parseAggregateField(field);
                result[field] = applyAggregateFunction(func, group, col);
            }
        });

        return result;
    });
}

function isAggregateField(field) {
    return /\w+\(\w+\)/.test(field);
}

function parseAggregateField(field) {
    const match = field.match(/(\w+)\((\w+)\)/);
    return { func: match[1], col: match[2] };
}

function applyAggregateFunction(func, group, col) {
    switch (func.toUpperCase()) {
        case 'COUNT':
            return group.length;
        case 'SUM':
            return group.reduce((acc, row) => acc + row[col], 0);
        case 'AVG':
            return group.reduce((acc, row) => acc + row[col], 0) / group.length;
        case 'MAX':
            return Math.max(...group.map(row => row[col]));
        case 'MIN':
            return Math.min(...group.map(row => row[col]));
        default:
            throw new Error(`Unsupported aggregate function: ${func}`);
    }
}


module.exports = executeSELECTQuery;