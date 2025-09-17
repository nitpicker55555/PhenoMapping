import pandas as pd
import psycopg2
from datetime import datetime
import re

# 数据库连接参数
conn_params = {
    'host': 'localhost',
    'database': 'pheno_new',
    'user': 'postgres',
    'password': '',
    'port': '5432'
}

# 物候期列映射
phenophase_mapping = {
    'Die Knospen brechen.': 3,
    'Die ersten Blätter sind entfaltet.': 4,
    'Allgemeine Belaubung.': 16,
    'Die ersten Blätter zeigen die farbliche Färbung.': 31,
    'Alle Blätter zeigen die farbliche Färbung.': 31,
    'Das abfallen der Blätter beginnt.': 32,
    'Alle Blätter sind abgefallen.': 32,
    'Die ersten Blüthen sind entfaltet.': 5,
    'Allgemeines Blühen.': 6,
    'Sämtliche Blüthen sind verblüht.': 7,
    'Die ersten Früchte sind reif.': 29,
    'Allgemeine Fruchtreife.': 29,
    'Sämtliche Früchte sind abgefallen.': 30,
}

def parse_date(date_str, year=1856):
    """解析日期字符串"""
    if pd.isna(date_str) or date_str == '-' or date_str == '':
        return None
    
    date_str = str(date_str).strip()
    
    if 'Tage' in date_str or 'Wochen' in date_str or 'ohne' in date_str:
        return None
    
    try:
        if re.match(r'^\d{1,2}\.\d{1,2}$', date_str):
            parts = date_str.split('.')
            return datetime(year, int(parts[1]), int(parts[0]))
    except ValueError:
        return None
    
    return None

def extract_species_info(species_str):
    """从物种字符串中提取德文名和拉丁名"""
    if pd.isna(species_str):
        return None, None
    
    species_str = species_str.strip()
    
    # 跳过非物种条目
    if any(skip in species_str for skip in ['Tabelle', 'Seite', 'Molcher']):
        return None, None
    
    # 移除数字前缀
    species_str = re.sub(r'^\d+\.\s*', '', species_str)
    
    german_name = None
    latin_name = None
    
    # 提取德文名和拉丁名
    if '(' in species_str and ')' in species_str:
        match = re.search(r'(.+?)\s*\((.+?)\)', species_str)
        if match:
            part1 = match.group(1).strip()
            part2 = match.group(2).strip()
            
            if re.match(r'^[A-Z][a-z]+\s+[a-z]+', part1):
                latin_name = part1
                german_name = part2
            else:
                german_name = part1
                latin_name = part2
    elif ',' in species_str or ':' in species_str:
        delimiter = ',' if ',' in species_str else ':'
        parts = species_str.split(delimiter, 1)
        if len(parts) == 2:
            part1 = parts[0].strip()
            part2 = parts[1].strip()
            
            if re.match(r'^[A-Z][a-z]+\s+[a-z]+', part1):
                latin_name = part1
                german_name = part2
            else:
                german_name = part1
                latin_name = part2
    else:
        if re.match(r'^[A-Z][a-z]+\s+[a-z]+', species_str):
            latin_name = species_str
        else:
            german_name = species_str
    
    # 清理名称
    if german_name:
        german_name = german_name.strip()
    if latin_name:
        latin_name = latin_name.strip()
    
    return german_name, latin_name

def main():
    print("=" * 80)
    print("导入未映射的物种数据到 pheno_new")
    print("=" * 80)
    
    # 连接数据库
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    
    # 1. 读取数据
    print("\n1. 读取数据文件...")
    csv_df = pd.read_csv('/Users/puzhen/Desktop/pheno/PhenoMapping/merged_phenology_data.csv')
    mapping_df = pd.read_csv('/Users/puzhen/Desktop/pheno/PhenoMapping/species_mapping_final.csv')
    
    # 获取已映射的物种
    mapped_species = set(mapping_df['csv_name'].unique())
    
    # 获取CSV中所有唯一的物种
    all_csv_species = csv_df['Name der Gewächse'].dropna().unique()
    
    # 找出未映射的物种
    unmapped_species = [sp for sp in all_csv_species if sp not in mapped_species]
    
    print(f"   总物种数: {len(all_csv_species)}")
    print(f"   已映射物种: {len(mapped_species)}")
    print(f"   未映射物种: {len(unmapped_species)}")
    
    # 2. 获取当前最大的物种ID
    cursor.execute("SELECT MAX(CAST(id AS INTEGER)) FROM dwd_species")
    max_species_id = cursor.fetchone()[0] or 999
    new_species_id = max_species_id + 1
    
    print(f"\n2. 创建新物种记录（起始ID: {new_species_id}）...")
    
    # 为未映射的物种创建记录
    species_id_mapping = {}
    
    for species_str in unmapped_species:
        german_name, latin_name = extract_species_info(species_str)
        
        # 跳过无效条目
        if not german_name and not latin_name:
            continue
        
        # 使用原始字符串作为德文名（如果没有解析出来）
        if not german_name:
            german_name = species_str
        
        # 创建物种记录
        try:
            cursor.execute("""
                INSERT INTO dwd_species (id, species_name_de, species_name_en, species_name_la)
                VALUES (%s, %s, %s, %s)
            """, (
                str(new_species_id),
                german_name[:100] if len(german_name) > 100 else german_name,
                None,  # 英文名未知
                latin_name[:100] if latin_name and len(latin_name) > 100 else latin_name
            ))
            
            species_id_mapping[species_str] = new_species_id
            new_species_id += 1
            
        except Exception as e:
            print(f"   警告: 无法创建物种 '{species_str}': {e}")
            continue
    
    conn.commit()
    print(f"   成功创建 {len(species_id_mapping)} 个新物种记录")
    
    # 3. 获取当前最大的观测ID
    cursor.execute("SELECT MAX(CAST(id AS INTEGER)) FROM dwd_observation")
    max_obs_id = cursor.fetchone()[0] or 0
    observation_id = max_obs_id + 1
    
    # 4. 导入未映射物种的观测数据
    print(f"\n3. 导入观测数据（起始ID: {observation_id}）...")
    
    observations = []
    processed_count = 0
    
    for idx, row in csv_df.iterrows():
        species_name = row.get('Name der Gewächse')
        
        # 只处理未映射的物种
        if pd.isna(species_name) or species_name not in species_id_mapping:
            continue
        
        station_id = row.get('Index')
        if pd.isna(station_id):
            continue
        
        species_id = species_id_mapping[species_name]
        station_id = str(int(station_id))
        year = 1856
        
        # 处理每个物候期
        for column, phase_id in phenophase_mapping.items():
            if column in row:
                date_str = row[column]
                date_obj = parse_date(date_str, year)
                
                if date_obj:
                    observation = {
                        'id': str(observation_id),
                        'station_id': station_id,
                        'reference_year': str(year),
                        'quality_level_id': '10',
                        'species_id': str(species_id),
                        'phase_id': str(phase_id),
                        'date': date_obj.strftime('%Y-%m-%d %H:%M:%S.000000'),
                        'quality_byte_id': '1',
                        'day_of_year': str(date_obj.timetuple().tm_yday),
                        'source': 'historical_csv_unmapped',
                        'dataset': 'historical_1856',
                        'partition': 'historical'
                    }
                    observations.append(observation)
                    observation_id += 1
        
        processed_count += 1
    
    print(f"   处理了 {processed_count} 条记录")
    print(f"   生成了 {len(observations)} 条观测记录")
    
    # 5. 批量插入观测数据
    if observations:
        print("\n4. 插入观测数据...")
        
        batch_size = 1000
        for i in range(0, len(observations), batch_size):
            batch = observations[i:i + batch_size]
            
            values = []
            for obs in batch:
                values.append((
                    obs['id'], obs['station_id'], obs['reference_year'],
                    obs['quality_level_id'], obs['species_id'], obs['phase_id'],
                    obs['date'], obs['quality_byte_id'], obs['day_of_year'],
                    obs['source'], obs['dataset'], obs['partition']
                ))
            
            cursor.executemany(
                """INSERT INTO dwd_observation 
                (id, station_id, reference_year, quality_level_id, species_id, 
                 phase_id, date, quality_byte_id, day_of_year, source, dataset, partition) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                values
            )
            
            print(f"   已插入 {min(i + batch_size, len(observations))} 条记录...")
        
        conn.commit()
    
    # 6. 验证结果
    print("\n5. 验证结果...")
    
    # 总观测记录数
    cursor.execute("SELECT COUNT(*) FROM dwd_observation")
    total_obs = cursor.fetchone()[0]
    
    # 新增的物种数据
    cursor.execute("""
        SELECT COUNT(*) FROM dwd_observation 
        WHERE source = 'historical_csv_unmapped'
    """)
    new_obs = cursor.fetchone()[0]
    
    # 总物种数
    cursor.execute("SELECT COUNT(DISTINCT species_id) FROM dwd_observation")
    total_species = cursor.fetchone()[0]
    
    print(f"   总观测记录: {total_obs}")
    print(f"   新增观测记录: {new_obs}")
    print(f"   总物种数: {total_species}")
    
    # 显示新增的物种
    if species_id_mapping:
        print("\n   新增的物种示例:")
        cursor.execute("""
            SELECT DISTINCT s.id, s.species_name_de, s.species_name_la, COUNT(o.id) as obs_count
            FROM dwd_species s
            JOIN dwd_observation o ON s.id = o.species_id
            WHERE CAST(s.id AS INTEGER) > %s
            GROUP BY s.id, s.species_name_de, s.species_name_la
            ORDER BY obs_count DESC
            LIMIT 10
        """, (max_species_id,))
        
        for row in cursor.fetchall():
            print(f"     ID={row[0]}: {row[1]} ({row[2]}) - {row[3]}条观测")
    
    cursor.close()
    conn.close()
    
    print("\n未映射物种数据导入完成！")

if __name__ == "__main__":
    main()