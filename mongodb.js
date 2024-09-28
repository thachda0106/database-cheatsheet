MONGODB: // --- MongoDB JavaScript Snippet ---

// --- Connect to MongoDB ---
const { MongoClient } = require('mongodb');
const uri = 'mongodb://localhost:27017';
const client = new MongoClient(uri);

async function run() {
    try {
        await client.connect();
        const database = client.db('weatherDB');
        const citiesCollection = database.collection('cities');
        const weatherCollection = database.collection('weather');

        // --- Create collections with validation ---
        await createCitiesWithValidation();

        // --- Enable Sharding ---
        await enableSharding('weatherDB');

        // --- Insert documents ---
        await insertInitialData();

        // --- Update documents ---
        await updateWeatherData();

        // --- Delete documents ---
        await deleteWeatherData();

        // --- Select documents ---
        await selectDocuments();

        // --- Join (Aggregation Pipeline) ---
        await joinData();

        // --- Aggregate Functions ---
        await aggregateFunctions();

        // --- Count and Group By ---
        await countAndGroupBy();

        // --- Count - Group By and Filter ---
        await countGroupByAndFilter();

        // --- Views (Simulated with Aggregation) ---
        await createView();

        // --- UNION-like functionality (Aggregation Framework) ---
        await unionLikeFunctionality();

        // --- Sharding ---
        await enableSharding('weatherDB');

        // --- Data Validation ---
        await createCitiesWithValidation();

        // --- Change Streams with Filtering ---
        await watchChangesWithFilter();

        // --- Aggregation Pipeline Optimization ---
        await complexAggregation();

        // --- Stored Procedures / Server-Side JavaScript ---
        await runStoredProcedure();

        // --- Geospatial Indexing and Queries ---
        await findCitiesWithinDistance([102.0, 2.0], 100); // Example coordinates

        // --- Full-text Search with Advanced Options ---
        await advancedTextSearch('Hayward');

        // --- Security Features ---
        await createUserWithPermissions();

        // --- Backup and Restore ---
        console.log('Backup command (run in shell): mongodump --db weatherDB --out /path/to/backup');
        
    } finally {
        await client.close();
    }
}

// --- Function Definitions ---

const createCitiesWithValidation = async () => {
    await database.createCollection('cities', {
        validator: {
            $jsonSchema: {
                bsonType: 'object',
                required: ['name', 'location'],
                properties: {
                    name: { bsonType: 'string' },
                    location: { 
                        bsonType: 'object', 
                        properties: { 
                            type: { enum: ['Point'], required: true }, 
                            coordinates: { bsonType: 'array', items: { bsonType: 'double' } } 
                        } 
                    },
                }
            }
        }
    });
    console.log('Cities collection created with validation.');
};

const enableSharding = async (dbName) => {
    await client.db("admin").command({ enableSharding: dbName });
    console.log(`Sharding enabled for database: ${dbName}`);
};

const insertInitialData = async () => {
    await weatherCollection.insertMany([
        { date: '1994-11-29', city: 'Hayward', temp_hi: 54, temp_lo: 37 },
        { date: '1994-11-30', city: 'Hayward', temp_hi: 56, temp_lo: 39 },
        // Add more initial data as needed
    ]);
    console.log('Initial data inserted into weather collection.');
};

const updateWeatherData = async () => {
    await weatherCollection.updateMany(
        { date: { $gt: '1994-11-28' } },
        { $inc: { temp_hi: -2, temp_lo: -2 } }
    );
    console.log('Weather data updated.');
};

const deleteWeatherData = async () => {
    await weatherCollection.deleteOne({ city: 'Hayward' });
    console.log('Hayward data deleted from weather collection.');
};

const selectDocuments = async () => {
    const distinctCities = await weatherCollection.distinct('city');
    console.log('Distinct cities:', distinctCities);

    const tempAverage = await weatherCollection.aggregate([
        { $group: { _id: '$city', temp_avg: { $avg: { $avg: ['$temp_hi', '$temp_lo'] } } } }
    ]).toArray();
    console.log('Average temperatures by city:', tempAverage);
};

const joinData = async () => {
    const result = await weatherCollection.aggregate([
        {
            $lookup: {
                from: 'cities',
                localField: 'city',
                foreignField: 'name',
                as: 'city_info'
            }
        }
    ]).toArray();
    console.log('Join results:', result);
};

const aggregateFunctions = async () => {
    const maxTempLo = await weatherCollection.find({}).sort({ temp_lo: -1 }).limit(1).toArray();
    console.log('Max temp_lo:', maxTempLo);
};

const countAndGroupBy = async () => {
    const result = await weatherCollection.aggregate([
        { $group: { _id: '$city', count: { $sum: 1 }, max_temp_lo: { $max: '$temp_lo' } } }
    ]).toArray();
    console.log('Count and Group By result:', result);
};

const countGroupByAndFilter = async () => {
    const result = await weatherCollection.aggregate([
        { $group: { _id: '$city', count: { $sum: 1 }, max_temp_lo: { $max: '$temp_lo' } } },
        { $match: { max_temp_lo: { $lt: 40 } } }
    ]).toArray();
    console.log('Count, Group By and Filter result:', result);
};

const createView = async () => {
    // MongoDB doesn't support views directly through JavaScript, simulate with aggregation
    const myView = await weatherCollection.aggregate([
        {
            $lookup: {
                from: 'cities',
                localField: 'city',
                foreignField: 'name',
                as: 'city_info'
            }
        },
        {
            $project: {
                _id: 0,
                city: 1,
                temp_lo: 1,
                temp_hi: 1,
                prcp: 1,
                date: 1,
                location: { $arrayElemAt: ['$city_info.location', 0] }
            }
        }
    ]).toArray();
    console.log('Simulated View Results:', myView);
};

const unionLikeFunctionality = async () => {
    const result1 = await weatherCollection.find({ city: 'Hayward' }).toArray();
    const result2 = await weatherCollection.find({ city: 'Oakland' }).toArray();
    const unionResult = [...result1, ...result2];
    console.log('Union-like results:', unionResult);
};

const watchChangesWithFilter = async () => {
    const changeStream = weatherCollection.watch([{ $match: { 'fullDocument.temp_lo': { $gt: 40 } } }]);
    changeStream.on('change', (change) => {
        console.log('Change detected (filtered):', change);
    });
};

const complexAggregation = async () => {
    const result = await weatherCollection.aggregate([
        {
            $facet: {
                byCity: [
                    { $group: { _id: '$city', avg_temp_hi: { $avg: '$temp_hi' }, count: { $sum: 1 } } }
                ],
                byDate: [
                    { $group: { _id: '$date', total_prcp: { $sum: '$prcp' } } }
                ]
            }
        }
    ]).toArray();
    console.log('Complex Aggregation Results:', result);
};

const runStoredProcedure = async () => {
    // This is a placeholder for running server-side JavaScript, more suited for a MongoDB shell
    console.log('Stored Procedure result placeholder.');
};

const findCitiesWithinDistance = async (location, distance) => {
    const nearbyCities = await citiesCollection.find({
        location: {
            $geoWithin: {
                $centerSphere: [location, distance / 3963.2] // distance in miles
            }
        }
    }).toArray();
    console.log('Cities within distance:', nearbyCities);
};

const advancedTextSearch = async (searchString) => {
    const results = await citiesCollection.find({
        $text: {
            $search: searchString,
            $caseSensitive: false,
            $diacriticSensitive: false
        }
    }).toArray();
    console.log('Advanced Text Search Results:', results);
};

const createUserWithPermissions = async () => {
    await client.db('admin').command({
        createUser: 'newUser',
        pwd: 'password123',
        roles: [{ role: 'readWrite', db: 'weatherDB' }]
    });
    console.log('User created with readWrite permissions.');
};

// Run the function
run().catch(console.error);
