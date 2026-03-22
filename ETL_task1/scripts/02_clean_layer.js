// 02_clean_layer.js - buduje warstwe clean z raw

print("czyszcze stare kolekcje clean...");
db.clean_zones.drop();

// EWR to lotnisko w New Jersey, w danych jest blednie jako oddzielny borough
db.runCommand({
    aggregate: "raw_zones",
    pipeline: [
        {
            $addFields: {
                Borough: { $cond: [{ $eq: ["$Borough", "EWR"] }, "New Jersey", "$Borough"] },
                is_airport: { $in: ["$Zone", ["JFK Airport", "LaGuardia Airport", "Newark Airport"]] }
            }
        },
        { $merge: "clean_zones" }
    ],
    cursor: {}
});

print(`clean_zones: ${db.clean_zones.countDocuments()} dokumentow`);

print("\nbuduje clean_trips...");

db.runCommand({
    aggregate: "raw_trips",
    pipeline: [
        {
            $match: {
                fare_amount: { $gt: 0, $lte: 500 },
                trip_distance: { $gt: 0, $lte: 200 },
                passenger_count: { $gte: 1, $lte: 8 },
                PULocationID: { $ne: null },
                DOLocationID: { $ne: null },
                tpep_pickup_datetime: { $ne: null },
                tpep_dropoff_datetime: { $ne: null }
            }
        },
        // wczesny $project - mniejsze dokumenty przez kolejne etapy
        {
            $project: {
                _id: 0,
                VendorID: 1,
                tpep_pickup_datetime: 1,
                tpep_dropoff_datetime: 1,
                pickup_hour: { $hour: "$tpep_pickup_datetime" },
                pickup_date: { $dateToString: { format: "%Y-%m-%d", date: "$tpep_pickup_datetime" } },
                pickup_year: { $year: "$tpep_pickup_datetime" },
                // roznica timestampow w MongoDB jest w milisekundach
                trip_duration_min: {
                    $round: [{ $divide: [{ $subtract: ["$tpep_dropoff_datetime", "$tpep_pickup_datetime"] }, 60000] }, 1]
                },
                passenger_count: 1,
                trip_distance: 1,
                PULocationID: 1,
                DOLocationID: 1,
                RatecodeID: 1,
                payment_type: 1,
                fare_amount: 1,
                tip_amount: 1,
                tip_pct: { $round: [{ $multiply: [{ $divide: ["$tip_amount", "$fare_amount"] }, 100] }, 1] },
                total_amount: 1,
                // starsze pliki nie maja tych kolumn
                congestion_surcharge: { $ifNull: ["$congestion_surcharge", 0] },
                airport_fee: { $ifNull: ["$airport_fee", 0] },
                __sourceFile: 1
            }
        },
        {
            $match: {
                trip_duration_min: { $gte: 1, $lte: 300 }
            }
        },
        { $out: "clean_trips" }
    ],
    allowDiskUse: true,
    cursor: {}
});

db.clean_trips.createIndex({ PULocationID: 1 });
db.clean_trips.createIndex({ DOLocationID: 1 });
db.clean_trips.createIndex({ pickup_hour: 1 });
db.clean_trips.createIndex({ pickup_date: 1 });
db.clean_trips.createIndex({ pickup_year: 1 });

const raw = db.raw_trips.countDocuments();
const clean = db.clean_trips.countDocuments();
print(`\nraw_trips:   ${raw.toLocaleString()}`);
print(`clean_trips: ${clean.toLocaleString()} (${((clean / raw) * 100).toFixed(1)}% zachowanych)`);
print(`odfiltrowano: ${(raw - clean).toLocaleString()} rekordow`);
