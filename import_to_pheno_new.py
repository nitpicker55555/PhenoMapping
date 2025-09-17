import pandas as pd
import psycopg2
from datetime import datetime
import re
import uuid
import os

# 数据库连接参数
conn_params_old = {
    'host': 'localhost',
    'database': 'pheno',
    'user': 'postgres',
    'password': '',
    'port': '5432'
}

conn_params_new = {
    'host': 'localhost',
    'database': 'pheno_new',
    'user': 'postgres',
    'password': '',
    'port': '5432'
}

# 物候期列名映射到phase_id
phenophase_mapping = {
    'Die Knospen brechen.': 3,  # Austrieb Beginn
    'Die ersten Blätter sind entfaltet.': 4,  # Blattentfaltung Beginn
    'Allgemeine Belaubung.': 16,  # Blattbildung Beginn (approximate)
    'Die ersten Blätter zeigen die farbliche Färbung.': 31,  # herbstliche Blattverfärbung
    'Alle Blätter zeigen die farbliche Färbung.': 31,  # herbstliche Blattverfärbung
    'Das abfallen der Blätter beginnt.': 32,  # herbstlicher Blattfall
    'Alle Blätter sind abgefallen.': 32,  # herbstlicher Blattfall
    'Die ersten Blüthen sind entfaltet.': 5,  # Blüte Beginn
    'Allgemeines Blühen.': 6,  # Vollblüte
    'Sämtliche Blüthen sind verblüht.': 7,  # Blüte Ende
    'Die ersten Früchte sind reif.': 29,  # Fruchtreife (general)
    'Allgemeine Fruchtreife.': 29,  # Fruchtreife
    'Sämtliche Früchte sind abgefallen.': 30,  # After fruit fall (approximate)
}

def parse_date(date_str, year=None):
    """解析日期字符串 (DD.MM 格式) 并添加年份"""
    if pd.isna(date_str) or date_str == '-' or date_str == '':
        return None
    
    # 移除多余的空格
    date_str = str(date_str).strip()
    
    # 尝试匹配 DD.MM 格式
    match = re.match(r'^(\d{1,2})\.(\d{1,2})$', date_str)
    if match:
        day = int(match.group(1))
        month = int(match.group(2))
        
        # 使用默认年份（如果没有提供）
        if year is None:
            year = 1850  # 历史数据的默认年份
        
        try:
            # 验证日期有效性
            date_obj = datetime(year, month, day)
            return date_obj.strftime('%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            # 无效日期
            return None
    
    return None

def get_station_from_description(description, location=None):
    """从站点描述中提取或创建站点ID"""
    if pd.isna(description):
        # Use location as fallback if description is missing
        if location and not pd.isna(location):
            station_key = str(location)
            station_id = f"LOC_{abs(hash(station_key)) % 1000:03d}"
            return station_id
        return 'HIST_001'  # 默认历史站点ID
    
    # 简化描述作为站点标识
    station_key = description[:50] if len(description) > 50 else description
    # 生成一个基于描述的伪ID
    station_id = f"HIST_{abs(hash(station_key)) % 1000:03d}"
    return station_id

def main():
    print("开始数据导入流程...")
    print("-" * 50)
    
    # 连接两个数据库
    conn_old = psycopg2.connect(**conn_params_old)
    conn_new = psycopg2.connect(**conn_params_new)
    cursor_old = conn_old.cursor()
    cursor_new = conn_new.cursor()
    
    # 1. 读取CSV数据和映射表
    print("\n1. 读取数据文件...")
    # Use relative paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_df = pd.read_csv(os.path.join(script_dir, 'merged_phenology_data.csv'))
    mapping_df = pd.read_csv(os.path.join(script_dir, 'species_mapping_final.csv'))
    
    print(f"   CSV记录数: {len(csv_df)}")
    print(f"   映射物种数: {len(mapping_df)}")
    
    # 2. 从原数据库复制必要的参考数据
    print("\n2. 复制参考数据...")
    
    # 复制物种数据
    cursor_old.execute("SELECT * FROM dwd_species")
    species_data = cursor_old.fetchall()
    for row in species_data:
        cursor_new.execute(
            "INSERT INTO dwd_species VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
            row
        )
    
    # 复制物候期数据
    cursor_old.execute("SELECT * FROM dwd_phase")
    phase_data = cursor_old.fetchall()
    for row in phase_data:
        cursor_new.execute(
            "INSERT INTO dwd_phase VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            row
        )
    
    # 复制质量级别数据
    cursor_old.execute("SELECT * FROM dwd_quality_level")
    quality_data = cursor_old.fetchall()
    for row in quality_data:
        cursor_new.execute(
            "INSERT INTO dwd_quality_level VALUES (%s, %s) ON CONFLICT DO NOTHING",
            row
        )
    
    # 复制质量字节数据
    cursor_old.execute("SELECT * FROM dwd_quality_byte")
    quality_byte_data = cursor_old.fetchall()
    for row in quality_byte_data:
        cursor_new.execute(
            "INSERT INTO dwd_quality_byte VALUES (%s, %s) ON CONFLICT DO NOTHING",
            row
        )
    
    # 添加历史数据的about信息
    cursor_new.execute(
        "INSERT INTO dwd_about VALUES (%s, %s) ON CONFLICT DO NOTHING",
        ('source', 'Historical phenology data from CSV import')
    )
    cursor_new.execute(
        "INSERT INTO dwd_about VALUES (%s, %s) ON CONFLICT DO NOTHING",
        ('import_date', datetime.now().strftime('%Y-%m-%d'))
    )
    
    conn_new.commit()
    print("   参考数据复制完成")
    
    # 3. 创建站点数据
    print("\n3. 处理站点信息...")
    # Get unique combinations of location and description
    unique_locations = csv_df[['Location', 'Genaue Bezeichnung der Standorte']].drop_duplicates()
    station_mapping = {}
    location_to_station = {}
    
    for i, row in unique_locations.iterrows():
        location = row['Location']
        station_desc = row['Genaue Bezeichnung der Standorte']
        
        # Create station ID using both location and description
        station_id = get_station_from_description(station_desc, location)
        
        # Map both description and location to station ID
        if pd.notna(station_desc):
            station_mapping[station_desc] = station_id
        if pd.notna(location):
            location_to_station[location] = station_id
        
        # Determine station name
        if pd.notna(location):
            station_name = location
        else:
            station_name = "Unknown"
        
        # Determine area description
        area_desc = None
        if pd.notna(station_desc):
            area_desc = station_desc[:100] if len(station_desc) > 100 else station_desc
        elif pd.notna(location):
            area_desc = location
        
        # 插入站点数据
        cursor_new.execute("""
            INSERT INTO dwd_station 
            (id, station_name, latitude, longitude, altitude, area_group_code, 
             area_group, area_code, area, station_date_abandoned, state)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            station_id,
            station_name,
            None,  # latitude
            None,  # longitude
            None,  # altitude
            None,  # area_group_code
            'Historical',  # area_group
            None,  # area_code
            area_desc,  # area
            None,  # station_date_abandoned
            'Historical'  # state
        ))
    
    conn_new.commit()
    print(f"   创建了 {len(set(list(station_mapping.values()) + list(location_to_station.values())))} 个站点")
    
    # 4. 转换数据格式并插入
    print("\n4. 转换并插入观测数据...")
    
    # 创建映射字典
    species_mapping_dict = dict(zip(mapping_df['csv_name'], mapping_df['db_species_id']))
    
    observation_id = 1
    inserted_count = 0
    skipped_count = 0
    
    for idx, row in csv_df.iterrows():
        # 获取物种ID
        species_name = row['Name der Gewächse']
        if pd.isna(species_name) or species_name not in species_mapping_dict:
            skipped_count += 1
            continue
        
        species_id = str(int(species_mapping_dict[species_name]))
        
        # 获取站点ID - first try description, then location
        station_desc = row['Genaue Bezeichnung der Standorte']
        location = row.get('Location', None)
        
        station_id = None
        if pd.notna(station_desc) and station_desc in station_mapping:
            station_id = station_mapping[station_desc]
        elif pd.notna(location) and location in location_to_station:
            station_id = location_to_station[location]
        else:
            station_id = 'HIST_001'
        
        # 获取年份 - first try Date column, then use 1856 as default
        year = 1856  # Default year for this historical dataset
        if 'Date' in row and pd.notna(row['Date']):
            date_str = str(row['Date'])
            # Try to extract year from date string (e.g., "25.11.1856")
            year_match = re.search(r'(\d{4})', date_str)
            if year_match:
                year = int(year_match.group(1))
        
        # 处理每个物候期
        for phenophase_col, phase_id in phenophase_mapping.items():
            if phenophase_col in row:
                date_str = row[phenophase_col]
                
                # 解析日期
                date_parsed = parse_date(date_str, year)
                if date_parsed is None:
                    continue
                
                # 计算年积日
                try:
                    date_obj = datetime.strptime(date_parsed[:10], '%Y-%m-%d')
                    day_of_year = date_obj.timetuple().tm_yday
                except:
                    day_of_year = None
                
                # 插入观测记录
                try:
                    cursor_new.execute("""
                        INSERT INTO dwd_observation
                        (id, station_id, reference_year, quality_level_id, species_id,
                         phase_id, date, quality_byte_id, day_of_year, source, dataset, partition)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        str(observation_id),
                        station_id,
                        str(year),
                        '10',  # 默认质量级别
                        species_id,
                        str(phase_id),
                        date_parsed,
                        '1',  # 默认质量字节
                        str(day_of_year) if day_of_year else None,
                        'csv_import',
                        'historical',
                        'historical'
                    ))
                    
                    observation_id += 1
                    inserted_count += 1
                    
                    if inserted_count % 100 == 0:
                        conn_new.commit()
                        print(f"   已插入 {inserted_count} 条记录...")
                        
                except Exception as e:
                    # 忽略插入错误，继续处理
                    pass
    
    # 最终提交
    conn_new.commit()
    
    print(f"\n导入完成!")
    print(f"   成功插入: {inserted_count} 条观测记录")
    print(f"   跳过记录: {skipped_count} 条（无映射）")
    
    # 显示数据统计
    cursor_new.execute("SELECT COUNT(*) FROM dwd_observation")
    total_obs = cursor_new.fetchone()[0]
    
    cursor_new.execute("SELECT COUNT(DISTINCT species_id) FROM dwd_observation")
    total_species = cursor_new.fetchone()[0]
    
    cursor_new.execute("SELECT COUNT(DISTINCT station_id) FROM dwd_observation")
    total_stations = cursor_new.fetchone()[0]
    
    print(f"\n数据库统计:")
    print(f"   总观测记录: {total_obs}")
    print(f"   物种数: {total_species}")
    print(f"   站点数: {total_stations}")
    
    # 关闭连接
    cursor_old.close()
    cursor_new.close()
    conn_old.close()
    conn_new.close()
    
    print("\n数据导入流程完成！")

if __name__ == "__main__":
    main()