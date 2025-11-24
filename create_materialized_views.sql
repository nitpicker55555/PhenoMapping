-- 创建物化视图以加速常用查询
-- 这些视图会预先计算好统计结果

-- 1. 物种统计视图
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_species_stats AS
SELECT
    s.id,
    s.species_name_de,
    s.species_name_en,
    s.species_name_la,
    COUNT(o.id) as observation_count
FROM dwd_species s
LEFT JOIN dwd_observation o ON s.id = o.species_id
GROUP BY s.id, s.species_name_de, s.species_name_en, s.species_name_la;

-- 为物化视图创建索引
CREATE INDEX IF NOT EXISTS idx_mv_species_stats_id ON mv_species_stats(id);
CREATE INDEX IF NOT EXISTS idx_mv_species_stats_count ON mv_species_stats(observation_count);

-- 2. 站点统计视图
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_station_stats AS
SELECT
    s.id,
    s.station_name,
    s.latitude,
    s.longitude,
    s.altitude,
    s.state,
    s.area_group,
    s.area,
    COUNT(o.id) as observation_count
FROM dwd_station s
LEFT JOIN dwd_observation o ON s.id = o.station_id
GROUP BY s.id, s.station_name, s.latitude, s.longitude, s.altitude, s.state, s.area_group, s.area;

CREATE INDEX IF NOT EXISTS idx_mv_station_stats_id ON mv_station_stats(id);
CREATE INDEX IF NOT EXISTS idx_mv_station_stats_count ON mv_station_stats(observation_count);

-- 3. 物候期统计视图
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_phase_stats AS
SELECT
    p.id,
    p.phase_name_de,
    p.phase_name_en,
    COUNT(o.id) as observation_count
FROM dwd_phase p
LEFT JOIN dwd_observation o ON p.id = o.phase_id
GROUP BY p.id, p.phase_name_de, p.phase_name_en;

CREATE INDEX IF NOT EXISTS idx_mv_phase_stats_id ON mv_phase_stats(id);
CREATE INDEX IF NOT EXISTS idx_mv_phase_stats_count ON mv_phase_stats(observation_count);

-- 刷新所有物化视图
REFRESH MATERIALIZED VIEW mv_species_stats;
REFRESH MATERIALIZED VIEW mv_station_stats;
REFRESH MATERIALIZED VIEW mv_phase_stats;

-- 显示物化视图大小
SELECT
    schemaname,
    matviewname as name,
    pg_size_pretty(pg_relation_size(schemaname||'.'||matviewname)) as size
FROM pg_matviews
WHERE schemaname = 'public';
