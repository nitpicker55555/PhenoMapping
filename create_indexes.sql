-- 数据库索引优化脚本
-- 为 pheno 数据库创建索引以提升查询性能

-- 1. dwd_observation 表的索引（主要表，1700万条记录）
CREATE INDEX IF NOT EXISTS idx_observation_station_id ON dwd_observation(station_id);
CREATE INDEX IF NOT EXISTS idx_observation_species_id ON dwd_observation(species_id);
CREATE INDEX IF NOT EXISTS idx_observation_phase_id ON dwd_observation(phase_id);
CREATE INDEX IF NOT EXISTS idx_observation_year ON dwd_observation(reference_year);
CREATE INDEX IF NOT EXISTS idx_observation_quality ON dwd_observation(quality_level_id);

-- 组合索引，用于趋势分析查询
CREATE INDEX IF NOT EXISTS idx_observation_species_phase ON dwd_observation(species_id, phase_id);
CREATE INDEX IF NOT EXISTS idx_observation_year_station ON dwd_observation(reference_year, station_id);

-- 2. dwd_station 表的索引
CREATE INDEX IF NOT EXISTS idx_station_state ON dwd_station(state);
CREATE INDEX IF NOT EXISTS idx_station_area ON dwd_station(area);

-- 3. dwd_species 表的索引
CREATE INDEX IF NOT EXISTS idx_species_name_de ON dwd_species(species_name_de);
CREATE INDEX IF NOT EXISTS idx_species_name_en ON dwd_species(species_name_en);
CREATE INDEX IF NOT EXISTS idx_species_name_la ON dwd_species(species_name_la);

-- 4. dwd_phase 表的索引
CREATE INDEX IF NOT EXISTS idx_phase_name_de ON dwd_phase(phase_name_de);
CREATE INDEX IF NOT EXISTS idx_phase_name_en ON dwd_phase(phase_name_en);

-- 5. dwd_species_group 表的索引
CREATE INDEX IF NOT EXISTS idx_species_group_species_id ON dwd_species_group(species_id);

-- 更新统计信息
ANALYZE dwd_observation;
ANALYZE dwd_station;
ANALYZE dwd_species;
ANALYZE dwd_phase;
ANALYZE dwd_quality_level;
ANALYZE dwd_species_group;

-- 显示索引创建结果
SELECT
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) as index_size
FROM pg_stat_user_indexes
WHERE schemaname = 'public'
ORDER BY tablename, indexname;
