// 01_raw_layer.js - weryfikacja warstwy raw i indeksy

const zonesCount = db.raw_zones.countDocuments();
print("raw_zones:", zonesCount, "dokumentow");
if (zonesCount === 0) {
    print("raw_zones jest pusta - uruchom najpierw load_data.py");
    quit(1);
}
printjson(db.raw_zones.findOne());

const tripsCount = db.raw_trips.countDocuments();
print("\nraw_trips:", tripsCount, "dokumentow");
if (tripsCount === 0) {
    print("raw_trips jest pusta - uruchom najpierw load_data.py");
    quit(1);
}
printjson(db.raw_trips.findOne());

db.raw_trips.createIndex({ PULocationID: 1 });
db.raw_trips.createIndex({ DOLocationID: 1 });
db.raw_trips.createIndex({ tpep_pickup_datetime: 1 });
db.raw_trips.createIndex({ __sourceFile: 1 });
// pola uzywane w $match clean layer - bez tego pelny scan
db.raw_trips.createIndex({ fare_amount: 1 });
db.raw_trips.createIndex({ trip_distance: 1 });
db.raw_trips.createIndex({ passenger_count: 1 });
print("\nindeksy ok");

print("\nrekordy per plik:");
db.raw_trips.aggregate([
    { $group: { _id: "$__sourceFile", count: { $sum: 1 } } },
    { $sort: { _id: 1 } }
]).forEach(d => print(`  ${d._id}: ${d.count.toLocaleString()}`));

// jedna agregacja zamiast osobnego countDocuments per pole
// wykrywa schema drift miedzy latami
const fields = [
    "VendorID", "passenger_count", "trip_distance",
    "PULocationID", "DOLocationID", "fare_amount",
    "tip_amount", "total_amount", "congestion_surcharge", "airport_fee"
];

print("\nobecnosc pol (% dokumentow):");
const presenceResult = db.raw_trips.aggregate([
    {
        $group: {
            _id: null,
            ...Object.fromEntries(fields.map(f => [
                f, { $sum: { $cond: [{ $ifNull: [`$${f}`, false] }, 1, 0] } }
            ]))
        }
    }
]).toArray()[0];

fields.forEach(f => {
    const pct = ((presenceResult[f] / tripsCount) * 100).toFixed(1);
    print(`  ${f}: ${pct}%`);
});
