from flask import Flask, render_template, jsonify, request, send_file
import psycopg2
import json
from datetime import datetime, timedelta
import os
import re
import subprocess
from pathlib import Path
from odf import text, teletype
from odf.opendocument import load, OpenDocumentText
from odf.table import Table, TableRow, TableCell
import base64
from PIL import Image
import io
from odt_editor import ODTEditor
from geocoder import geocode_location

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'database': 'pheno',
    'user': 'postgres',
    'password': '9417941',
    'port': '5432'
}

# pheno_new数据库配置
DB_CONFIG_NEW = {
    'host': 'localhost',
    'database': 'pheno_new',
    'user': 'postgres',
    'password': '9417941',
    'port': '5432'
}

def get_db_connection():
    """获取数据库连接"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def get_db_connection_new():
    """获取pheno_new数据库连接"""
    try:
        conn = psycopg2.connect(**DB_CONFIG_NEW)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def dict_fetchall(cursor):
    """将查询结果转换为字典列表"""
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

@app.route('/')
def index():
    """主页"""
    return render_template('index.html')

@app.route('/geography')
def geography():
    """地理分布页面"""
    return render_template('geography.html')

@app.route('/timeline')
def timeline():
    """时间分析页面"""
    return render_template('timeline.html')

@app.route('/species')
def species_page():
    """物种研究页面"""
    return render_template('species.html')

@app.route('/quality')
def quality():
    """数据质量页面"""
    return render_template('quality.html')

@app.route('/transcription-editor')
def transcription_editor():
    """Transcription file editor page"""
    return render_template('transcription_editor.html')

@app.route('/distribution')
def distribution():
    """数据分布页面"""
    return render_template('distribution.html')

# API 端点
@app.route('/api/overview')
def api_overview():
    """数据概览API"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor()
        
        # 获取基本统计
        stats = {}
        
        # 总观测数
        cursor.execute("SELECT COUNT(*) FROM dwd_observation")
        stats['total_observations'] = cursor.fetchone()[0]
        
        # 站点数
        cursor.execute("SELECT COUNT(*) FROM dwd_station")
        stats['total_stations'] = cursor.fetchone()[0]
        
        # 物种数
        cursor.execute("SELECT COUNT(*) FROM dwd_species")
        stats['total_species'] = cursor.fetchone()[0]
        
        # 物候期数
        cursor.execute("SELECT COUNT(*) FROM dwd_phase")
        stats['total_phases'] = cursor.fetchone()[0]
        
        # 时间范围
        cursor.execute("SELECT MIN(reference_year), MAX(reference_year) FROM dwd_observation")
        year_range = cursor.fetchone()
        stats['year_range'] = f"{year_range[0]}-{year_range[1]}"
        
        # 最新观测
        cursor.execute("""
            SELECT COUNT(*) FROM dwd_observation 
            WHERE reference_year = (SELECT MAX(reference_year) FROM dwd_observation)
        """)
        stats['latest_year_observations'] = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return jsonify(stats)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stations')
def api_stations():
    """站点数据API - 使用物化视图优化性能"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        cursor = conn.cursor()

        # 使用物化视图快速获取站点统计信息
        cursor.execute("""
            SELECT
                id, station_name, latitude, longitude,
                altitude, state, area_group, area,
                observation_count
            FROM mv_station_stats
            ORDER BY observation_count DESC
        """)

        stations = dict_fetchall(cursor)

        cursor.close()
        conn.close()

        return jsonify(stations)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/species')
def api_species():
    """物种数据API - 使用物化视图优化性能"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        cursor = conn.cursor()

        # 使用物化视图快速获取物种统计信息
        cursor.execute("""
            SELECT
                s.id, s.species_name_de, s.species_name_en, s.species_name_la,
                sg.group_name,
                s.observation_count
            FROM mv_species_stats s
            LEFT JOIN dwd_species_group sg ON s.id = sg.species_id
            ORDER BY observation_count DESC
        """)

        species = dict_fetchall(cursor)

        cursor.close()
        conn.close()

        return jsonify(species)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/phases')
def api_phases():
    """物候期数据API - 使用物化视图优化性能"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        cursor = conn.cursor()

        # 使用物化视图快速获取物候期统计信息
        cursor.execute("""
            SELECT
                id, phase_name_de, phase_name_en,
                observation_count
            FROM mv_phase_stats
            ORDER BY observation_count DESC
        """)

        phases = dict_fetchall(cursor)

        cursor.close()
        conn.close()

        return jsonify(phases)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/observations')
def api_observations():
    """观测数据API（支持筛选）"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor()
        
        # 获取筛选参数
        station_id = request.args.get('station_id')
        species_id = request.args.get('species_id')
        phase_id = request.args.get('phase_id')
        year_start = request.args.get('year_start')
        year_end = request.args.get('year_end')
        limit = int(request.args.get('limit', 1000))
        
        # 构建查询
        where_conditions = []
        params = []
        
        if station_id:
            where_conditions.append("o.station_id = %s")
            params.append(station_id)
        if species_id:
            where_conditions.append("o.species_id = %s")
            params.append(species_id)
        if phase_id:
            where_conditions.append("o.phase_id = %s")
            params.append(phase_id)
        if year_start:
            where_conditions.append("o.reference_year >= %s")
            params.append(year_start)
        if year_end:
            where_conditions.append("o.reference_year <= %s")
            params.append(year_end)
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        query = f"""
            SELECT 
                o.id, o.station_id, o.reference_year, o.species_id, 
                o.phase_id, o.date, o.day_of_year,
                st.station_name, st.latitude, st.longitude,
                sp.species_name_en, sp.species_name_de,
                ph.phase_name_en, ph.phase_name_de
            FROM dwd_observation o
            JOIN dwd_station st ON o.station_id = st.id
            JOIN dwd_species sp ON o.species_id = sp.id
            JOIN dwd_phase ph ON o.phase_id = ph.id
            {where_clause}
            ORDER BY o.reference_year DESC, o.day_of_year
            LIMIT %s
        """
        
        params.append(limit)
        cursor.execute(query, params)
        
        observations = dict_fetchall(cursor)
        
        cursor.close()
        conn.close()
        
        return jsonify(observations)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/trends')
def api_trends():
    """趋势分析API"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor()
        
        species_id = request.args.get('species_id')
        phase_id = request.args.get('phase_id')
        
        if not species_id or not phase_id:
            return jsonify({'error': 'species_id and phase_id are required'}), 400
        
        cursor.execute("""
            SELECT 
                reference_year,
                AVG(CAST(day_of_year AS INTEGER)) as avg_day_of_year,
                COUNT(*) as observation_count
            FROM dwd_observation
            WHERE species_id = %s AND phase_id = %s
            GROUP BY reference_year
            ORDER BY reference_year
        """, (species_id, phase_id))
        
        trends = dict_fetchall(cursor)
        
        cursor.close()
        conn.close()
        
        return jsonify(trends)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/quality')
def api_quality():
    """数据质量统计API"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor()
        
        # 质量等级分布
        cursor.execute("""
            SELECT 
                ql.id, ql.description,
                COUNT(o.id) as count
            FROM dwd_quality_level ql
            LEFT JOIN dwd_observation o ON ql.id = o.quality_level_id
            GROUP BY ql.id, ql.description
            ORDER BY count DESC
        """)
        
        quality_levels = dict_fetchall(cursor)
        
        # 按年份的质量分布
        cursor.execute("""
            SELECT 
                o.reference_year,
                ql.description,
                COUNT(o.id) as count
            FROM dwd_observation o
            JOIN dwd_quality_level ql ON o.quality_level_id = ql.id
            WHERE o.reference_year >= '1925' AND o.reference_year <= '2020'
            GROUP BY o.reference_year, ql.description
            ORDER BY o.reference_year, ql.description
        """)
        
        quality_by_year = dict_fetchall(cursor)
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'quality_levels': quality_levels,
            'quality_by_year': quality_by_year
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/species-by-phase')
def api_species_by_phase():
    """根据phase搜索species API"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor()
        
        phase_id = request.args.get('phase_id')
        if not phase_id:
            return jsonify({'error': 'phase_id is required'}), 400
        
        # 查询具有指定phase的所有species
        cursor.execute("""
            SELECT DISTINCT
                s.id, s.species_name_de, s.species_name_en, s.species_name_la,
                sg.group_name,
                COUNT(DISTINCT o.id) as observation_count,
                COUNT(DISTINCT o.station_id) as station_count,
                MIN(o.reference_year) as first_year,
                MAX(o.reference_year) as last_year
            FROM dwd_observation o
            JOIN dwd_species s ON o.species_id = s.id
            LEFT JOIN dwd_species_group sg ON s.id = sg.species_id
            WHERE o.phase_id = %s
            GROUP BY s.id, s.species_name_de, s.species_name_en, s.species_name_la, sg.group_name
            ORDER BY observation_count DESC
        """, (phase_id,))
        
        species = dict_fetchall(cursor)
        
        cursor.close()
        conn.close()
        
        return jsonify(species)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/species-phases/<species_id>')
def api_species_phases(species_id):
    """获取特定species的所有phases"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor()
        
        # 获取该species的所有phases及其观测统计
        cursor.execute("""
            SELECT DISTINCT
                p.id as phase_id,
                p.phase_name_de,
                p.phase_name_en,
                COUNT(o.id) as observation_count,
                MIN(o.reference_year) as first_year,
                MAX(o.reference_year) as last_year,
                AVG(CAST(o.day_of_year AS INTEGER)) as avg_day_of_year
            FROM dwd_observation o
            JOIN dwd_phase p ON o.phase_id = p.id
            WHERE o.species_id = %s
            GROUP BY p.id, p.phase_name_de, p.phase_name_en
            ORDER BY observation_count DESC
        """, (species_id,))
        
        phases = dict_fetchall(cursor)
        
        cursor.close()
        conn.close()
        
        return jsonify(phases)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/pheno-new/species')
def api_pheno_new_species():
    """获取pheno_new数据库中的物种数据，并标记在pheno数据库中是否存在"""
    conn_new = get_db_connection_new()
    conn = get_db_connection()
    if not conn_new or not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor_new = conn_new.cursor()
        cursor = conn.cursor()
        
        # 获取pheno_new中的所有物种及其观测地点
        cursor_new.execute("""
            SELECT 
                s.id as species_id,
                s.species_name_en,
                s.species_name_la,
                s.species_name_de,
                COUNT(DISTINCT o.id) as observation_count,
                STRING_AGG(DISTINCT st.station_name, ', ' ORDER BY st.station_name) as locations
            FROM dwd_species s
            LEFT JOIN dwd_observation o ON s.id = o.species_id
            LEFT JOIN dwd_station st ON o.station_id = st.id
            GROUP BY s.id, s.species_name_en, s.species_name_la, s.species_name_de
            ORDER BY COUNT(DISTINCT o.id) DESC
        """)
        
        new_species = dict_fetchall(cursor_new)
        
        # 获取pheno数据库中的所有物种名称
        cursor.execute("""
            SELECT DISTINCT species_name_de FROM dwd_species
            UNION
            SELECT DISTINCT species_name_en FROM dwd_species
            UNION 
            SELECT DISTINCT species_name_la FROM dwd_species
        """)
        
        existing_species_names = set(row[0] for row in cursor.fetchall() if row[0])
        
        # 标记每个物种是否在pheno数据库中存在
        for species in new_species:
            # 检查任何一个名称是否在pheno数据库中存在
            species['exists_in_pheno'] = (
                species['species_name_en'] in existing_species_names or
                species['species_name_la'] in existing_species_names or
                species['species_name_de'] in existing_species_names
            )
            # 选择一个非空的名称作为显示名称
            species['species_name'] = (
                species['species_name_en'] or 
                species['species_name_la'] or 
                species['species_name_de'] or 
                'Unknown'
            )
            # 标准化locations显示 - 将N/A改为Unknown
            if not species['locations'] or species['locations'] == 'N/A':
                species['locations'] = 'Unknown'
        
        cursor_new.close()
        cursor.close()
        conn_new.close()
        conn.close()
        
        return jsonify(new_species)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/pheno-new/species-phases/<species_name>')
def api_pheno_new_species_phases(species_name):
    """获取pheno_new中特定物种的物候期数据，并与pheno数据库中的数据对比"""
    conn_new = get_db_connection_new()
    conn = get_db_connection()
    if not conn_new or not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor_new = conn_new.cursor()
        cursor = conn.cursor()
        
        # 获取pheno_new中该物种的物候期数据
        cursor_new.execute("""
            SELECT DISTINCT
                p.id as phase_id,
                p.phase_name_en,
                p.phase_name_de,
                COUNT(o.id) as observation_count,
                MIN(o.date) as start_date,
                MAX(o.date) as end_date
            FROM dwd_observation o
            JOIN dwd_species s ON o.species_id = s.id
            JOIN dwd_phase p ON o.phase_id = p.id
            WHERE (s.species_name_en = %s OR s.species_name_la = %s OR s.species_name_de = %s)
            GROUP BY p.id, p.phase_name_en, p.phase_name_de
            ORDER BY p.phase_name_en
        """, (species_name, species_name, species_name))
        
        new_phases = dict_fetchall(cursor_new)
        
        # 获取pheno_new中的时间序列数据 - 按月份显示1856年数据
        cursor_new.execute("""
            SELECT 
                p.phase_name_en,
                p.phase_name_de,
                CAST(o.reference_year AS INTEGER) + (EXTRACT(MONTH FROM o.date::date) - 1) / 12.0 as year,
                AVG(CAST(o.day_of_year AS INTEGER)) as avg_day_of_year,
                COUNT(o.id) as observation_count
            FROM dwd_observation o
            JOIN dwd_species s ON o.species_id = s.id
            JOIN dwd_phase p ON o.phase_id = p.id
            WHERE (s.species_name_en = %s OR s.species_name_la = %s OR s.species_name_de = %s)
                AND o.date IS NOT NULL
            GROUP BY p.phase_name_en, p.phase_name_de, o.reference_year, EXTRACT(MONTH FROM o.date::date)
            ORDER BY p.phase_name_en, year
        """, (species_name, species_name, species_name))
        
        new_time_series = dict_fetchall(cursor_new)
        
        # 查找该物种在pheno数据库中的对应ID（可能有多个匹配）
        cursor.execute("""
            SELECT id, species_name_de, species_name_en, species_name_la 
            FROM dwd_species 
            WHERE species_name_de = %s OR species_name_en = %s OR species_name_la = %s
        """, (species_name, species_name, species_name))
        
        pheno_species_matches = dict_fetchall(cursor)
        
        pheno_phases = []
        pheno_time_series = []
        if pheno_species_matches:
            # 获取pheno数据库中的物候期数据
            species_ids = [s['id'] for s in pheno_species_matches]
            placeholders = ','.join(['%s'] * len(species_ids))
            cursor.execute(f"""
                SELECT DISTINCT
                    p.id as phase_id,
                    p.phase_name_de,
                    p.phase_name_en,
                    COUNT(o.id) as observation_count,
                    AVG(CAST(o.day_of_year AS INTEGER)) as avg_day_of_year
                FROM dwd_observation o
                JOIN dwd_phase p ON o.phase_id = p.id
                WHERE o.species_id IN ({placeholders})
                GROUP BY p.id, p.phase_name_de, p.phase_name_en
                ORDER BY p.phase_name_de
            """, species_ids)
            
            pheno_phases = dict_fetchall(cursor)
            
            # 获取pheno数据库的时间序列数据
            cursor.execute(f"""
                SELECT 
                    p.phase_name_en,
                    p.phase_name_de,
                    o.reference_year as year,
                    AVG(CAST(o.day_of_year AS INTEGER)) as avg_day_of_year,
                    COUNT(o.id) as observation_count
                FROM dwd_observation o
                JOIN dwd_phase p ON o.phase_id = p.id
                WHERE o.species_id IN ({placeholders})
                    AND o.day_of_year IS NOT NULL
                GROUP BY p.phase_name_en, p.phase_name_de, o.reference_year
                ORDER BY p.phase_name_en, o.reference_year
            """, species_ids)
            
            pheno_time_series = dict_fetchall(cursor)
        
        cursor_new.close()
        cursor.close()
        conn_new.close()
        conn.close()
        
        return jsonify({
            'pheno_new_phases': new_phases,
            'pheno_new_time_series': new_time_series,
            'pheno_phases': pheno_phases,
            'pheno_time_series': pheno_time_series,
            'pheno_species_matches': pheno_species_matches
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/pheno-new/locations')
def api_pheno_new_locations():
    """获取pheno_new数据库中的地理位置并转换为坐标"""
    conn_new = get_db_connection_new()
    if not conn_new:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        cursor_new = conn_new.cursor()

        # Get unique locations and their observation counts from pheno_new
        cursor_new.execute("""
            SELECT
                st.station_name as location,
                COUNT(DISTINCT o.id) as observation_count
            FROM dwd_station st
            INNER JOIN dwd_observation o ON st.id = o.station_id
            WHERE st.area_group = 'Historical'
            GROUP BY st.station_name
            ORDER BY st.station_name
        """)

        locations_data = dict_fetchall(cursor_new)

        # Geocode locations and filter out those without coordinates
        geocoded_locations = []
        for loc in locations_data:
            location_name = loc['location']
            if location_name and not location_name.startswith('Historical Station'):
                geocoded = geocode_location(location_name)
                if geocoded:
                    geocoded['observations'] = loc['observation_count']
                    geocoded_locations.append(geocoded)

        cursor_new.close()
        conn_new.close()

        return jsonify(geocoded_locations)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/data-distribution')
def api_data_distribution():
    """获取数据时空分布统计 - 同时从pheno和pheno_new数据库"""
    conn = get_db_connection()
    conn_new = get_db_connection_new()

    if not conn:
        return jsonify({'error': 'Pheno database connection failed'}), 500

    try:
        cursor = conn.cursor()

        # ===== PHENO数据库数据 =====
        # 获取年份-地区的观测数量分布（按1年）
        cursor.execute("""
            SELECT
                CAST(reference_year AS INTEGER) as year,
                s.state,
                COUNT(o.id) as observation_count
            FROM dwd_observation o
            JOIN dwd_station s ON o.station_id = s.id
            WHERE s.state IS NOT NULL AND s.state != ''
            GROUP BY CAST(reference_year AS INTEGER), s.state
            ORDER BY year, state
        """)

        pheno_time_location_dist = dict_fetchall(cursor)

        # 获取月份分布（day_of_year转换为月份）
        cursor.execute("""
            SELECT
                CAST(reference_year AS INTEGER) as year,
                CASE
                    WHEN CAST(day_of_year AS INTEGER) <= 31 THEN 1
                    WHEN CAST(day_of_year AS INTEGER) <= 59 THEN 2
                    WHEN CAST(day_of_year AS INTEGER) <= 90 THEN 3
                    WHEN CAST(day_of_year AS INTEGER) <= 120 THEN 4
                    WHEN CAST(day_of_year AS INTEGER) <= 151 THEN 5
                    WHEN CAST(day_of_year AS INTEGER) <= 181 THEN 6
                    WHEN CAST(day_of_year AS INTEGER) <= 212 THEN 7
                    WHEN CAST(day_of_year AS INTEGER) <= 243 THEN 8
                    WHEN CAST(day_of_year AS INTEGER) <= 273 THEN 9
                    WHEN CAST(day_of_year AS INTEGER) <= 304 THEN 10
                    WHEN CAST(day_of_year AS INTEGER) <= 334 THEN 11
                    ELSE 12
                END as month,
                COUNT(o.id) as observation_count
            FROM dwd_observation o
            WHERE day_of_year IS NOT NULL AND day_of_year != ''
            GROUP BY CAST(reference_year AS INTEGER), month
            ORDER BY year, month
        """)

        pheno_month_dist = dict_fetchall(cursor)

        # 获取数据覆盖范围统计
        cursor.execute("""
            SELECT
                MIN(CAST(reference_year AS INTEGER)) as min_year,
                MAX(CAST(reference_year AS INTEGER)) as max_year,
                COUNT(DISTINCT station_id) as station_count,
                COUNT(DISTINCT species_id) as species_count,
                COUNT(DISTINCT phase_id) as phase_count
            FROM dwd_observation
        """)

        pheno_coverage = dict_fetchone(cursor)

        cursor.close()
        conn.close()

        # ===== PHENO_NEW数据库数据 =====
        pheno_new_time_location_dist = []
        pheno_new_month_dist = []
        pheno_new_coverage = None

        if conn_new:
            cursor_new = conn_new.cursor()

            # 获取年份-地区的观测数量分布（按1年）
            # For pheno_new, use station_name as location (consistent with /api/pheno-new/locations)
            # Filter out "Historical Station" prefixes to get actual location names
            cursor_new.execute("""
                SELECT
                    CAST(reference_year AS INTEGER) as year,
                    s.station_name as state,
                    COUNT(o.id) as observation_count
                FROM dwd_observation o
                JOIN dwd_station s ON o.station_id = s.id
                WHERE s.station_name IS NOT NULL
                  AND s.station_name != ''
                  AND s.area_group = 'Historical'
                  AND NOT s.station_name LIKE 'Historical Station%'
                GROUP BY CAST(reference_year AS INTEGER), s.station_name
                ORDER BY year, state
            """)

            pheno_new_time_location_dist = dict_fetchall(cursor_new)

            # 获取月份分布
            cursor_new.execute("""
                SELECT
                    CAST(reference_year AS INTEGER) as year,
                    CASE
                        WHEN CAST(day_of_year AS INTEGER) <= 31 THEN 1
                        WHEN CAST(day_of_year AS INTEGER) <= 59 THEN 2
                        WHEN CAST(day_of_year AS INTEGER) <= 90 THEN 3
                        WHEN CAST(day_of_year AS INTEGER) <= 120 THEN 4
                        WHEN CAST(day_of_year AS INTEGER) <= 151 THEN 5
                        WHEN CAST(day_of_year AS INTEGER) <= 181 THEN 6
                        WHEN CAST(day_of_year AS INTEGER) <= 212 THEN 7
                        WHEN CAST(day_of_year AS INTEGER) <= 243 THEN 8
                        WHEN CAST(day_of_year AS INTEGER) <= 273 THEN 9
                        WHEN CAST(day_of_year AS INTEGER) <= 304 THEN 10
                        WHEN CAST(day_of_year AS INTEGER) <= 334 THEN 11
                        ELSE 12
                    END as month,
                    COUNT(o.id) as observation_count
                FROM dwd_observation o
                WHERE day_of_year IS NOT NULL AND day_of_year != ''
                GROUP BY CAST(reference_year AS INTEGER), month
                ORDER BY year, month
            """)

            pheno_new_month_dist = dict_fetchall(cursor_new)

            # 获取数据覆盖范围统计
            cursor_new.execute("""
                SELECT
                    MIN(CAST(reference_year AS INTEGER)) as min_year,
                    MAX(CAST(reference_year AS INTEGER)) as max_year,
                    COUNT(DISTINCT station_id) as station_count,
                    COUNT(DISTINCT species_id) as species_count,
                    COUNT(DISTINCT phase_id) as phase_count
                FROM dwd_observation
            """)

            pheno_new_coverage = dict_fetchone(cursor_new)

            cursor_new.close()
            conn_new.close()

        return jsonify({
            'pheno': {
                'time_location_distribution': pheno_time_location_dist,
                'month_distribution': pheno_month_dist,
                'coverage': pheno_coverage
            },
            'pheno_new': {
                'time_location_distribution': pheno_new_time_location_dist,
                'month_distribution': pheno_new_month_dist,
                'coverage': pheno_new_coverage
            }
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

def dict_fetchone(cursor):
    """将单行查询结果转换为字典"""
    columns = [col[0] for col in cursor.description]
    row = cursor.fetchone()
    return dict(zip(columns, row)) if row else None

@app.route('/api/debug/pheno-new-stations')
def api_debug_pheno_new_stations():
    """Debug endpoint to check pheno_new station data"""
    conn_new = get_db_connection_new()

    if not conn_new:
        return jsonify({'error': 'Pheno_new database connection failed'}), 500

    try:
        cursor_new = conn_new.cursor()

        # Check station table structure
        cursor_new.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'dwd_station'
            ORDER BY ordinal_position
        """)

        columns = dict_fetchall(cursor_new)

        # Get sample station data
        cursor_new.execute("""
            SELECT * FROM dwd_station LIMIT 5
        """)

        sample_stations = dict_fetchall(cursor_new)

        # Get stations with coordinates
        cursor_new.execute("""
            SELECT COUNT(*) as total_stations,
                   COUNT(latitude) as stations_with_lat,
                   COUNT(longitude) as stations_with_lon
            FROM dwd_station
        """)

        coord_stats = dict_fetchone(cursor_new)

        cursor_new.close()
        conn_new.close()

        return jsonify({
            'columns': columns,
            'sample_stations': sample_stations,
            'coordinate_stats': coord_stats
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/data-distribution-detailed')
def api_data_distribution_detailed():
    """获取详细的站点级别数据分布 - 用于地图和时间线可视化"""
    conn = get_db_connection()
    conn_new = get_db_connection_new()

    if not conn:
        return jsonify({'error': 'Pheno database connection failed'}), 500

    try:
        cursor = conn.cursor()

        # ===== PHENO数据库 - 站点级别聚合（使用物化视图优化） =====
        cursor.execute("""
            SELECT
                station_id,
                station_name,
                latitude,
                longitude,
                state,
                area,
                reference_year,
                observation_count
            FROM mv_station_yearly_stats
            ORDER BY station_name, reference_year
        """)

        pheno_station_yearly = dict_fetchall(cursor)

        cursor.close()
        conn.close()

        # ===== PHENO_NEW数据库 - 站点级别聚合 =====
        pheno_new_station_yearly = []

        if conn_new:
            cursor_new = conn_new.cursor()

            # First, get data without lat/lon filter
            cursor_new.execute("""
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
                WHERE s.station_name IS NOT NULL
                GROUP BY s.id, s.station_name, s.latitude, s.longitude, s.state, s.area, o.reference_year
                ORDER BY s.station_name, o.reference_year
            """)

            raw_data = dict_fetchall(cursor_new)

            # Geocode stations that don't have coordinates
            geocoded_cache = {}  # Cache to avoid geocoding same station multiple times

            for item in raw_data:
                # If coordinates exist and are valid, use them
                if item['latitude'] is not None and item['longitude'] is not None:
                    try:
                        lat = float(item['latitude'])
                        lon = float(item['longitude'])
                        if lat != 0 and lon != 0:  # Valid coordinates
                            pheno_new_station_yearly.append(item)
                            continue
                    except (ValueError, TypeError):
                        pass

                # Otherwise, try to geocode
                station_name = item['station_name']
                if station_name and not station_name.startswith('Historical Station'):
                    # Check cache first
                    if station_name not in geocoded_cache:
                        geocoded = geocode_location(station_name)
                        if geocoded:
                            geocoded_cache[station_name] = {
                                'latitude': geocoded['latitude'],
                                'longitude': geocoded['longitude']
                            }
                        else:
                            geocoded_cache[station_name] = None

                    # Use cached geocoded coordinates
                    if geocoded_cache[station_name]:
                        item['latitude'] = geocoded_cache[station_name]['latitude']
                        item['longitude'] = geocoded_cache[station_name]['longitude']
                        pheno_new_station_yearly.append(item)

            cursor_new.close()
            conn_new.close()

        return jsonify({
            'pheno': pheno_station_yearly,
            'pheno_new': pheno_new_station_yearly
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Transcription Editor API endpoints
TRANSCRIPTION_BASE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Transskriptionen')

@app.route('/api/transcription/folders')
def api_transcription_folders():
    """Get all folders list"""
    try:
        folders = []
        base_path = Path(TRANSCRIPTION_BASE_PATH)
        
        for folder in sorted(base_path.iterdir()):
            if folder.is_dir():
                # Extract folder index
                match = re.match(r'^(\d+)', folder.name)
                index = match.group(1) if match else None
                
                # Check for files
                has_odt = any(f.suffix == '.odt' for f in folder.iterdir() if f.is_file())
                has_tif = any(f.suffix == '.tif' for f in folder.iterdir() if f.is_file())
                is_tabelle = 'Tabelle' in folder.name
                
                folders.append({
                    'name': folder.name,
                    'index': index,
                    'hasOdt': has_odt,
                    'hasTif': has_tif,
                    'isTabelle': is_tabelle
                })
        
        return jsonify(folders)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/transcription/folder/<path:folder_name>')
def api_transcription_folder_contents(folder_name):
    """Get folder contents"""
    try:
        folder_path = Path(TRANSCRIPTION_BASE_PATH) / folder_name
        if not folder_path.exists():
            return jsonify({'error': 'Folder not found'}), 404
        
        files = []
        for file in sorted(folder_path.iterdir()):
            if file.is_file():
                if file.suffix in ['.odt', '.tif']:
                    files.append({
                        'name': file.name,
                        'type': file.suffix[1:]  # Remove the dot
                    })
        
        return jsonify({'files': files})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/transcription/image/<path:folder_name>/<path:file_name>')
def api_transcription_image(folder_name, file_name):
    """Get TIF image"""
    try:
        file_path = Path(TRANSCRIPTION_BASE_PATH) / folder_name / file_name
        if not file_path.exists():
            return jsonify({'error': 'File not found'}), 404
        
        # Convert TIF to PNG for web display
        img = Image.open(file_path)
        img_io = io.BytesIO()
        img.save(img_io, 'PNG')
        img_io.seek(0)
        
        return send_file(img_io, mimetype='image/png')
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/transcription/odt/<path:folder_name>/<path:file_name>')
def api_transcription_odt(folder_name, file_name):
    """Get ODT file content"""
    try:
        file_path = Path(TRANSCRIPTION_BASE_PATH) / folder_name / file_name
        if not file_path.exists():
            return jsonify({'error': 'File not found'}), 404
        
        # Extract tables from ODT
        doc = load(str(file_path))
        tables = []
        raw_content = []
        
        # Extract all text
        for p in doc.getElementsByType(text.P):
            raw_content.append(teletype.extractText(p))
        
        # Extract tables
        for table in doc.getElementsByType(Table):
            table_data = []
            for row in table.getElementsByType(TableRow):
                row_data = []
                for cell in row.getElementsByType(TableCell):
                    cell_text = ""
                    for p in cell.getElementsByType(text.P):
                        cell_text += teletype.extractText(p)
                    
                    repeat = cell.getAttribute("numbercolumnsrepeated")
                    if repeat:
                        repeat_count = int(repeat)
                    else:
                        repeat_count = 1
                    
                    for _ in range(repeat_count):
                        row_data.append(cell_text.strip())
                
                if row_data:
                    table_data.append(row_data)
            
            if table_data:
                tables.append(table_data)
        
        return jsonify({
            'raw_content': '\n'.join(raw_content),
            'tables': tables
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/transcription/annotations/<path:folder_name>/<path:file_name>')
def api_transcription_annotations(folder_name, file_name):
    """Get annotations for a specific file"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transcription_annotations (
                id SERIAL PRIMARY KEY,
                folder_name VARCHAR(500) NOT NULL,
                file_name VARCHAR(500) NOT NULL,
                annotation_text TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()

        # Get annotations for this file
        cursor.execute("""
            SELECT id, folder_name, file_name, annotation_text, created_at
            FROM transcription_annotations
            WHERE folder_name = %s AND file_name = %s
            ORDER BY created_at DESC
        """, (folder_name, file_name))

        annotations = dict_fetchall(cursor)

        cursor.close()
        conn.close()

        return jsonify({'annotations': annotations})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/transcription/annotations', methods=['POST'])
def api_transcription_add_annotation():
    """Add a new annotation"""
    try:
        data = request.json
        folder_name = data.get('folder_name')
        file_name = data.get('file_name')
        annotation_text = data.get('annotation_text')

        if not folder_name or not file_name or not annotation_text:
            return jsonify({'error': 'Missing required fields'}), 400

        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transcription_annotations (
                id SERIAL PRIMARY KEY,
                folder_name VARCHAR(500) NOT NULL,
                file_name VARCHAR(500) NOT NULL,
                annotation_text TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Insert annotation
        cursor.execute("""
            INSERT INTO transcription_annotations (folder_name, file_name, annotation_text)
            VALUES (%s, %s, %s)
            RETURNING id, created_at
        """, (folder_name, file_name, annotation_text))

        result = cursor.fetchone()
        conn.commit()

        cursor.close()
        conn.close()

        return jsonify({
            'success': True,
            'message': 'Annotation added successfully',
            'id': result[0],
            'created_at': result[1].isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0',port=9090)