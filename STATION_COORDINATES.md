# Station Coordinate Sources

Coordinates were assigned to 64 historical phenological observation stations (1856) in the `pheno_new` database. The stations are located in the Oberpfalz (Upper Palatinate) region of Bavaria, Germany, roughly between 48.5°–50.1° N and 11.0°–12.9° E.

## Coordinate Sources

| Source | Stations | Description |
|--------|----------|-------------|
| SHP exact match | 12 | Matched by place name (NAM) in the `Forstämter 5.shp` shapefile. Centroid of the corresponding forest district polygon. |
| SHP Forstamt match | 5 | Matched by forest office name (Forstamt) in the shapefile. Centroid of the district polygon. |
| Nominatim geocoding | 42 | Geocoded via OpenStreetMap Nominatim, queried as `"{station}, Oberpfalz, Bayern, Deutschland"`. |
| Estimated | 5 | Hamlets too small to geocode. Coordinates estimated from neighboring stations. |
| Skipped | 3 | Not real stations (`Unknown`, `Unknown_49-51`, `Bemerkungen`). No coordinates assigned. |

## SHP Exact Matches (12)

Stations where the NAM field in `Forstämter 5.shp` directly matches the station name. Coordinates are polygon centroids projected to WGS84 (EPSG:4326).

| Station | Latitude | Longitude | SHP NAM |
|---------|----------|-----------|---------|
| Allersberg | 49.16452 | 11.16597 | Allersberg |
| Breitenbrunn | 49.08192 | 11.63638 | Breitenbrunn |
| Münchsgrün | 49.94965 | 12.26403 | Münchsgrün |
| Schwaighausen | 49.16366 | 11.99932 | Schwaighausen |
| Sulzbürg | 49.30230 | 11.49617 | Sulzbürg |
| Taubenbach | 49.44537 | 11.82136 | Taubenbach |

## SHP Forstamt Matches (5)

Stations matched to a forest office district. The coordinates represent the district centroid, not the exact station location.

| Station | Latitude | Longitude | Forstamt |
|---------|----------|-----------|----------|
| Etzenricht | 49.67588 | 12.08697 | Weiden (NAM: Etzenricht in Kohlberg) |
| Hilpoltstein | 49.16452 | 11.16597 | Hilpoltstein (Oberpfalz) |
| Neumarkt | 49.30230 | 11.49617 | Neumarkt |
| Waldsassen | 49.94965 | 12.26403 | Waldsassen |
| Wernberg | 49.49389 | 12.16020 | Wernberg (NAM: Neunaigen oder Forst) |
| Forst | 49.49389 | 12.16020 | Wernberg (NAM: Neunaigen oder Forst) |

## Nominatim Geocoded (42)

Queried via `geopy.geocoders.Nominatim` with `"{station name}, Oberpfalz, Bayern, Deutschland"` (falling back to `Bayern` or `Deutschland` if no result).

| Station | Latitude | Longitude |
|---------|----------|-----------|
| Airischwand | 48.55784 | 11.82570 |
| Altenstadt | 49.71970 | 12.15896 |
| Berg | 49.33184 | 11.43801 |
| Bodenwöhr | 49.26606 | 12.35922 |
| Brunnau | 49.25378 | 11.17087 |
| Buchberg | 49.24052 | 11.44060 |
| Cham | 49.21782 | 12.66638 |
| Dreihöf (Plößberg) | 49.76485 | 12.33041 |
| Fichtelberg | 49.99986 | 11.85190 |
| Flossenbürg | 49.73299 | 12.34982 |
| Freihöls | 49.40084 | 12.01958 |
| Freudenberg | 49.27699 | 11.43927 |
| Geigant (Waldmünchen) | 49.32661 | 12.68291 |
| Grafenrief | 49.42155 | 12.68155 |
| Großbuchelberg | 49.97144 | 12.22315 |
| Heideck | 49.23122 | 11.54663 |
| Hocha (Waldmünchen) | 49.38497 | 12.68689 |
| Kastl | 49.67032 | 12.15638 |
| Lengenfeld | 49.23857 | 11.62995 |
| Mantel | 49.65387 | 12.04348 |
| Mähring | 49.90865 | 12.52847 |
| Neubäu | 49.64303 | 12.15244 |
| Neuneichen (zu Oberndorf) | 49.50000 | 12.18000 |
| Neustadt am Kulm | 49.82569 | 11.83532 |
| Nittenau | 49.19804 | 12.27188 |
| Painten | 48.99790 | 11.81282 |
| Parkstein | 49.70907 | 12.14197 |
| Pettenhofen | 49.37609 | 11.55010 |
| Pfaffenhofen | 49.26795 | 11.08113 |
| Pfreimdt | 49.49361 | 12.17987 |
| Pleystein | 49.64593 | 12.40998 |
| Pottenstetten | 49.23856 | 12.02052 |
| Prunn | 48.95503 | 11.72692 |
| Pullenried | 49.51790 | 12.45936 |
| Richtheim | 49.31791 | 11.45449 |
| Rötz | 49.34209 | 12.52788 |
| Seligenporten | 49.26365 | 11.30208 |
| Speinshart | 49.78718 | 11.82033 |
| Sulzbach | 49.24614 | 12.30962 |
| Tännesberg | 49.53224 | 12.32724 |
| Unterlind | 49.60896 | 12.31073 |
| Vilshofen | 49.29788 | 11.95297 |
| Waldmünchen | 49.37757 | 12.70621 |
| Wiesau | 49.91051 | 12.18440 |
| Wondreb | 49.90854 | 12.38517 |
| Zillendorf (Waldmünchen) | 49.35057 | 12.66761 |

## Estimated Coordinates (5)

These hamlets were too small to be found via geocoding or in the shapefile. Coordinates were estimated based on the location of neighboring stations and the regional context of the original documents.

| Station | Latitude | Longitude | Basis for Estimate |
|---------|----------|-----------|--------------------|
| Dürloch | 49.24000 | 12.00000 | Near Pottenstetten / Burglengenfeld area |
| Kaltenbrunn | 49.80000 | 11.90000 | Near Fichtelberg / Speinshart area |
| Kehlberg | 49.65000 | 12.10000 | Near Weiden area |
| Schramm | 49.50000 | 12.15000 | Near Weiden / Pfreimd area |
| Spitzel | 49.50000 | 12.15000 | Near Weiden / Pfreimd area |
| Unterzell | 49.22000 | 12.08000 | Near Nittenau (Oberpfalz context) |

## Skipped (3)

| Station | Reason |
|---------|--------|
| Unknown | Unidentified station from earlier CSV import |
| Unknown_49-51 | Unidentified station (ODT folders 49–51) |
| Bemerkungen | Not a station; contains remarks/notes |

## Data Sources

- **Shapefile**: `Forstämter 5.shp` — Historical Bavarian forest district boundaries (Lambert Conformal Conic projection, converted to WGS84)
- **Geocoding**: OpenStreetMap Nominatim via `geopy` Python library (queried February 2026)
