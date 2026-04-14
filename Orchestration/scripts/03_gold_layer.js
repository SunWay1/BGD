// 03_gold_layer.js - agregacje analityczne (warstwa gold)

print("czyszcze stare kolekcje gold...");
db.gold_zone_revenue.drop();
db.gold_hourly_demand.drop();
db.gold_top_routes.drop();

// --- gold_zone_revenue ---
// przychod i statystyki per strefa odbioru, z nazwa strefy przez $lookup
// SQL: SELECT z.Zone, SUM(total_amount), AVG(fare_amount) FROM trips JOIN zones ON PULocationID = LocationID GROUP BY PULocationID
print("\ngold_zone_revenue...");

db.runCommand({
    aggregate: "clean_trips",
    pipeline: [
        {
            $group: {
                _id:           "$PULocationID",
                trip_count:    { $sum: 1 },
                total_revenue: { $sum: "$total_amount" },
                avg_fare:      { $avg: "$fare_amount" },
                avg_tip_pct:   { $avg: "$tip_pct" },
                avg_distance:  { $avg: "$trip_distance" },
                avg_duration:  { $avg: "$trip_duration_min" }
            }
        },
        {
            $lookup: {
                from: "clean_zones", localField: "_id", foreignField: "LocationID", as: "zone_info"
            }
        },
        { $match: { zone_info: { $ne: [] } } },
        { $unwind: "$zone_info" },
        {
            $project: {
                _id:           0,
                location_id:   "$_id",
                zone:          "$zone_info.Zone",
                borough:       "$zone_info.Borough",
                is_airport:    "$zone_info.is_airport",
                trip_count:    1,
                total_revenue: { $round: ["$total_revenue", 2] },
                avg_fare:      { $round: ["$avg_fare", 2] },
                avg_tip_pct:   { $round: ["$avg_tip_pct", 1] },
                avg_distance:  { $round: ["$avg_distance", 2] },
                avg_duration:  { $round: ["$avg_duration", 1] }
            }
        },
        { $sort: { total_revenue: -1 } },
        { $merge: "gold_zone_revenue" }
    ],
    allowDiskUse: true,
    cursor: {}
});

print(`zaladowano: ${db.gold_zone_revenue.countDocuments()} stref`);
print("top 5 wg przychodu:");
db.gold_zone_revenue.find({}, { zone: 1, borough: 1, total_revenue: 1, trip_count: 1, _id: 0 })
    .sort({ total_revenue: -1 }).limit(5)
    .forEach(d => print(`  ${d.zone} (${d.borough}): $${d.total_revenue.toLocaleString()} | ${d.trip_count.toLocaleString()} kursow`));

// --- gold_hourly_demand ---
// popyt na przejazdy w podziale na godziny doby
// SQL: SELECT pickup_hour, COUNT(*), AVG(trip_distance) FROM trips GROUP BY pickup_hour
print("\ngold_hourly_demand...");

db.runCommand({
    aggregate: "clean_trips",
    pipeline: [
        {
            $group: {
                _id:          "$pickup_hour",
                trip_count:   { $sum: 1 },
                avg_distance: { $avg: "$trip_distance" },
                avg_revenue:  { $avg: "$total_amount" },
                avg_duration: { $avg: "$trip_duration_min" }
            }
        },
        {
            $project: {
                _id:          0,
                hour:         "$_id",
                trip_count:   1,
                avg_distance: { $round: ["$avg_distance", 2] },
                avg_revenue:  { $round: ["$avg_revenue", 2] },
                avg_duration: { $round: ["$avg_duration", 1] }
            }
        },
        { $sort: { hour: 1 } },
        { $merge: "gold_hourly_demand" }
    ],
    allowDiskUse: true,
    cursor: {}
});

print("godziny szczytu:");
db.gold_hourly_demand.find({}, { hour: 1, trip_count: 1, _id: 0 })
    .sort({ trip_count: -1 }).limit(3)
    .forEach(d => print(`  ${d.hour}:00 - ${d.trip_count.toLocaleString()} kursow`));

// --- gold_top_routes ---
// najpopularniejsze trasy odbior->docel, dwa $lookup (po jednym dla kazdej strefy)
// SQL: SELECT pu.Zone, do.Zone, COUNT(*) FROM trips JOIN zones pu ON PULocationID JOIN zones do ON DOLocationID GROUP BY PULocationID, DOLocationID HAVING COUNT(*) >= 1000
print("\ngold_top_routes...");

db.runCommand({
    aggregate: "clean_trips",
    pipeline: [
        {
            $group: {
                _id:          { PULocationID: "$PULocationID", DOLocationID: "$DOLocationID" },
                trip_count:   { $sum: 1 },
                avg_fare:     { $avg: "$fare_amount" },
                avg_distance: { $avg: "$trip_distance" }
            }
        },
        { $match: { trip_count: { $gte: 1000 } } },
        { $lookup: { from: "clean_zones", localField: "_id.PULocationID", foreignField: "LocationID", as: "pu_zone" } },
        { $match: { pu_zone: { $ne: [] } } },
        { $unwind: "$pu_zone" },
        { $lookup: { from: "clean_zones", localField: "_id.DOLocationID", foreignField: "LocationID", as: "do_zone" } },
        { $match: { do_zone: { $ne: [] } } },
        { $unwind: "$do_zone" },
        {
            $project: {
                _id:            0,
                pickup_zone:    "$pu_zone.Zone",
                dropoff_zone:   "$do_zone.Zone",
                pickup_borough: "$pu_zone.Borough",
                trip_count:     1,
                avg_fare:       { $round: ["$avg_fare", 2] },
                avg_distance:   { $round: ["$avg_distance", 2] }
            }
        },
        { $sort: { trip_count: -1 } },
        { $limit: 50 },
        { $merge: "gold_top_routes" }
    ],
    allowDiskUse: true,
    cursor: {}
});

print(`zaladowano: ${db.gold_top_routes.countDocuments()} tras (top 50)`);
print("top 5:");
db.gold_top_routes.find({}, { pickup_zone: 1, dropoff_zone: 1, trip_count: 1, _id: 0 })
    .sort({ trip_count: -1 }).limit(5)
    .forEach(d => print(`  ${d.pickup_zone} -> ${d.dropoff_zone}: ${d.trip_count.toLocaleString()}`));
