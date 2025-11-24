-- 优化 distribution 页面的物化视图
-- 为站点-年份级别的观测数据创建物化视图

-- 1. PHENO 数据库的站点-年份统计视图
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_station_yearly_stats AS
SELECT
    s.id as station_id,
    s.station_name,
    s.latitude,
    s.longitude,
    s.state,
    s.area,
    o.reference_year,
    COUNT(o.id) as observation_count
FROM dwd_observation o
JOIN dwd_station s ON o.station_id = s.id
WHERE s.latitude IS NOT NULL AND s.longitude IS NOT NULL
GROUP BY s.id, s.station_name, s.latitude, s.longitude, s.state, s.area, o.reference_year
ORDER BY s.station_name, o.reference_year;

-- 为物化视图创建索引
CREATE INDEX IF NOT EXISTS idx_mv_station_yearly_station ON mv_station_yearly_stats(station_id);
CREATE INDEX IF NOT EXISTS idx_mv_station_yearly_year ON mv_station_yearly_stats(reference_year);
CREATE INDEX IF NOT EXISTS idx_mv_station_yearly_composite ON mv_station_yearly_stats(station_id, reference_year);

-- 刷新物化视图
REFRESH MATERIALIZED VIEW mv_station_yearly_stats;

-- 显示物化视图大小
SELECT
    schemaname,
    matviewname as name,
    pg_size_pretty(pg_relation_size(schemaname||'.'||matviewname)) as size,
    (SELECT COUNT(*) FROM mv_station_yearly_stats) as row_count
FROM pg_matviews
WHERE schemaname = 'public' AND matviewname = 'mv_station_yearly_stats';
