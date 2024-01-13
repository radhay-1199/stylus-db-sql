function parseQuery(query) {
    // First, let's trim the query to remove any leading/trailing whitespaces
    query = query.trim();

    // extracting GROUP BY fields
    const groupByRegex = /\sGROUP BY\s(.+)/i;
    const groupByMatch = query.match(groupByRegex);

    let groupByFields = null;
    if (groupByMatch) {
        groupByFields = groupByMatch[1].split(',').map(field => field.trim());
    }

    // Split the query at the WHERE clause if it exists
    const whereSplit = query.split(/\sWHERE\s/i);
    const beforeWhere = whereSplit[0]; // Everything before WHERE clause
    const whereClause = whereSplit.length > 1 ? whereSplit[1].trim() : null;

    // Parse the SELECT part
    const selectRegex = /^SELECT\s(.+?)\sFROM\s/i;
    const selectMatch = beforeWhere.match(selectRegex);
    if (!selectMatch) {
        throw new Error('Invalid SELECT format');
    }
    const fields = selectMatch[1].trim().split(',').map(field => field.trim());

    // Remove the SELECT part from the query to isolate FROM and JOIN parts
    const fromAndJoinPart = beforeWhere.replace(selectRegex, '');

    // Parse the JOIN clause using parseJoinClause function
    const { joinType, joinTable, joinCondition } = parseJoinClause(fromAndJoinPart);

    // Extract the table name (it's the first word in fromAndJoinPart if there's no JOIN)
    const table = fromAndJoinPart.split(' ')[0].trim();

    // Parse the WHERE part if it exists
    let whereClauses = [];
    if (whereClause) {
        whereClauses = parseWhereClause(whereClause);
    }

    return {
        fields,
        table,
        joinType,
        joinTable,
        joinCondition,
        whereClauses,
        groupByFields
    };
}

function parseWhereClause(whereString) {
    const conditionRegex = /(.*?)(=|!=|>|<|>=|<=)(.*)/;
    return whereString.split(/ AND | OR /i).map(conditionString => {
        const match = conditionString.match(conditionRegex);
        if (match) {
            const [, field, operator, value] = match;
            return { field: field.trim(), operator, value: value.trim() };
        }
        throw new Error('Invalid WHERE clause format');
    });
}

function parseJoinClause(query) {
    const joinRegex = /\s(INNER|LEFT|RIGHT) JOIN\s(.+?)\sON\s([\w.]+)\s*=\s*([\w.]+)/i;
    const joinMatch = query.match(joinRegex);

    if (joinMatch) {
        return {
            joinType: joinMatch[1].trim(),
            joinTable: joinMatch[2].trim(),
            joinCondition: {
                left: joinMatch[3].trim(),
                right: joinMatch[4].trim()
            }
        };
    }

    return {
        joinType: null,
        joinTable: null,
        joinCondition: null
    };
}

module.exports = parseQuery;