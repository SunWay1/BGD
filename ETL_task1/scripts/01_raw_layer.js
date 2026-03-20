// 01_raw_layer.js — weryfikuje warstwe raw i tworzy indeksy

const zonesCount = db.raw_zones.countDocuments();
print("raw_zones:", zonesCount, "dokumentów");
if (zonesCount === 0) {
    print("ERROR: raw_zones jest pusta. Uruchom najpierw load_data.py");
    quit(1);
}
printjson(db.raw_zones.findOne());

const tripsCount = db.raw_trips.countDocuments();
print("\nraw_trips:", tripsCount, "dokumentów");
if (tripsCount === 0) {
    print("ERROR: raw_trips jest pusta. Uruchom najpierw load_data.py");
    quit(1);
}
printjson(db.raw_trips.findOne());

db.raw_trips.createIndex({ PULocationID: 1 });
db.raw_trips.createIndex({ DOLocationID: 1 });
db.raw_trips.createIndex({ tpep_pickup_datetime: 1 });
db.raw_trips.createIndex({ __sourceFile: 1 });
// indeksy na polach filtrujących w clean layer — bez nich $match skanuje całą kolekcję
db.raw_trips.createIndex({ fare_amount: 1 });
db.raw_trips.createIndex({ trip_distance: 1 });
db.raw_trips.createIndex({ passenger_count: 1 });
print("\nIndeksy utworzone.");

print("\nRekordy per plik źródłowy:");
db.raw_trips.aggregate([
    { $group: { _id: "$__sourceFile", count: { $sum: 1 } } },
    { $sort: { _id: 1 } }
]).forEach(d => print(`  ${d._id}: ${d.count.toLocaleString()}`));

// Jedna agregacja zamiast osobnego countDocuments dla każdego pola —
// sprawdza obecność kluczowych kolumn (wykrywa schema drift między latami)
const fields = [
    "VendorID", "passenger_count", "trip_distance",
    "PULocationID", "DOLocationID", "fare_amount",
    "tip_amount", "total_amount", "congestion_surcharge", "airport_fee"
];

print("\nObecność pól (% dokumentów):");
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
