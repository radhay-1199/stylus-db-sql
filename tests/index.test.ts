// tests/index.test.js
const readCSV = require('../src/csvReader');
const parseQuery = require('../src/queryParser');
const executeSELECTQuery = require('../src/index');

test('Basic Jest Test', () => {
    expect(1).toBe(1);
  });

test('Read CSV File', async () => {
    const data = await readCSV('./student.csv');
    expect(data.length).toBeGreaterThan(0);
    expect(data.length).toBe(4);
    expect(data[0].name).toBe('John');
    expect(data[0].age).toBe('30'); //ignore the string type here, we will fix this later
});

test('Parse SQL Query', () => {
    const query = 'SELECT id, name FROM student';
    const parsed = parseQuery(query);
    expect(parsed).toEqual({
        fields: ['id', 'name'],
        joinCondition: null,
        joinType: null,
        joinTable: null,
        table: 'student',
        whereClauses: [],
        groupByFields: null,
    });
});

test('Execute SQL Query with WHERE Clause', async () => {
    const query = 'SELECT id, name FROM student WHERE age = 25';
    const result = await executeSELECTQuery(query);
    expect(result.length).toBe(1);
    expect(result[0]).toHaveProperty('id');
    expect(result[0]).toHaveProperty('name');
    expect(result[0].id).toBe('2');
});

test('Parse SQL Query with Multiple WHERE Clauses', () => {
    const query = 'SELECT id, name FROM student WHERE age = 30 AND name = John';
    const parsed = parseQuery(query);
    expect(parsed).toEqual({
        fields: ['id', 'name'],
        joinCondition: null,
        joinType: null,
        joinTable: null,
        table: 'student',
        whereClauses: [{
            "field": "age",
            "operator": "=",
            "value": "30",
        }, {
            "field": "name",
            "operator": "=",
            "value": "John",
        }],
        groupByFields: null,
    });
});

test('Execute SQL Query with Multiple WHERE Clause', async () => {
    const query = 'SELECT id, name FROM student WHERE age = 30 AND name = John';
    const result = await executeSELECTQuery(query);
    expect(result.length).toBe(1);
    expect(result[0]).toEqual({ id: '1', name: 'John' });
});

test('Execute SQL Query with Greater Than', async () => {
    const queryWithGT = 'SELECT id FROM student WHERE age > 22';
    const result = await executeSELECTQuery(queryWithGT);
    expect(result.length).toEqual(3);
    expect(result[0]).toHaveProperty('id');
});

test('Execute SQL Query with Not Equal to', async () => {
    const queryWithGT = 'SELECT name FROM student WHERE age != 25';
    const result = await executeSELECTQuery(queryWithGT);
    expect(result.length).toEqual(3);
    expect(result[0]).toHaveProperty('name');
});

test('Parse SQL Query with INNER JOIN', () => {
    const query = 'SELECT student.name, enrollment.course FROM student INNER JOIN enrollment ON student.id=enrollment.student_id';
    const parsed = parseQuery(query);
    expect(parsed).toEqual({
        fields: ['student.name', 'enrollment.course'],
        table: 'student',
        joinType: 'INNER',
        joinTable: 'enrollment',
        joinCondition: { left: 'student.id', right: 'enrollment.student_id' },
        whereClauses: [],
        groupByFields: null,
    });
});

test('Parse SQL Query with INNER JOIN and WHERE Clause', () => {
    const query = 'SELECT student.name, enrollment.course FROM student INNER JOIN enrollment ON student.id=enrollment.student_id WHERE student.name = John';
    const parsed = parseQuery(query);
    expect(parsed).toEqual({
        fields: ['student.name', 'enrollment.course'],
        table: 'student',
        joinType: 'INNER',
        joinTable: 'enrollment',
        joinCondition: { left: 'student.id', right: 'enrollment.student_id' },
        whereClauses: [{ field: 'student.name', operator: '=', value: 'John' }],
        groupByFields: null,
    });
});

test('Execute SQL Query with INNER JOIN', async () => {
    const query = 'SELECT student.name, enrollment.course FROM student INNER JOIN enrollment ON student.id=enrollment.student_id';
    const result = await executeSELECTQuery(query);
    expect(result.length).toBeGreaterThan(0);
    expect(result[0].hasOwnProperty('student.name')).toBe(true);
    expect(result[0].hasOwnProperty('enrollment.course')).toBe(true);
});


test('Execute SQL Query with INNER JOIN and a WHERE Clause', async () => {
    const query = 'SELECT student.name, enrollment.course FROM student INNER JOIN enrollment ON student.id=enrollment.student_id WHERE student.name = John';
    const result = await executeSELECTQuery(query);
    expect(result.length).toBeGreaterThan(0);
    result.forEach(row => {
        expect(row['student.name']).toBe('John');
        expect(row.hasOwnProperty('enrollment.course')).toBe(true);
    });
});

test('Parse SQL Query with LEFT JOIN', () => {
    const query = 'SELECT student.name, enrollment.course FROM student LEFT JOIN enrollment ON student.id = enrollment.student_id';
    const parsed = parseQuery(query);
    expect(parsed).toEqual({
        fields: ['student.name', 'enrollment.course'],
        table: 'student',
        joinType: 'LEFT',
        joinTable: 'enrollment',
        joinCondition: { left: 'student.id', right: 'enrollment.student_id' },
        whereClauses: [],
        groupByFields: null,
    });
});

test('Parse SQL Query with RIGHT JOIN', () => {
    const query = 'SELECT student.name, enrollment.course FROM student RIGHT JOIN enrollment ON student.id = enrollment.student_id';
    const parsed = parseQuery(query);
    expect(parsed).toEqual({
        fields: ['student.name', 'enrollment.course'],
        table: 'student',
        joinType: 'RIGHT',
        joinTable: 'enrollment',
        joinCondition: { left: 'student.id', right: 'enrollment.student_id' },
        whereClauses: [],
        groupByFields: null,
    });
});

test('Execute SQL Query with LEFT JOIN', async () => {
    const query = 'SELECT student.name, enrollment.course FROM student LEFT JOIN enrollment ON student.id = enrollment.student_id';
    const result = await executeSELECTQuery(query);
    expect(result.length).toBeGreaterThan(0);
    expect(result.some(row => row['student.name'] && row['enrollment.course'] === null)).toBe(true);
});

test('Execute SQL Query with RIGHT JOIN', async () => {
    const query = 'SELECT student.name, enrollment.course FROM student RIGHT JOIN enrollment ON student.id = enrollment.student_id';
    const result = await executeSELECTQuery(query);
    expect(result.length).toBeGreaterThan(0);
    expect(result.some(row => row['enrollment.course'] && row['student.name'] === null)).toBe(true);
});

test('Execute SQL Query with Group By (Single Field) and Aggregate Function', async () => {
    const query = 'SELECT name, COUNT(age) as ageCount FROM student GROUP BY name';
    const result = await executeSELECTQuery(query);
    
    const johnGroup = result.find(group => group.name === 'John');
    const janeGroup = result.find(group => group.name === 'Jane');


    expect(johnGroup).toBeDefined();
    expect(janeGroup).toBeDefined();

    expect(johnGroup['COUNT(age) as ageCount']).toBe(1);
    expect(janeGroup['COUNT(age) as ageCount']).toBe(1); 
});