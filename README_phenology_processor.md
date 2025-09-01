# Phenology Data Processor

这个程序用于处理物候观察数据，从ODT文件中提取表格并合并成一个CSV文件。

## 功能

1. 自动查找所有包含"Tabelle"的文件夹中的ODT文件
2. 从每个ODT文件中提取所有表格
3. 将表格保存为单独的CSV文件，文件名包含源文件夹的索引号
4. 自动识别并合并所有16列的标准物候观察表格
5. 生成最终的合并CSV文件，包含所有观察数据

## 使用方法

### 方法1：使用简单包装脚本（推荐）
```bash
python3 process_phenology.py
```

或者指定输入目录：
```bash
python3 process_phenology.py /Users/puzhen/Downloads/Transskriptionen
```

### 方法2：直接运行主程序
```bash
python3 phenology_data_processor.py
```

## 输出文件

程序会在当前目录创建以下文件和文件夹：

1. `extracted_tables_csv/` - 包含所有从ODT文件提取的单独CSV文件
2. `merged_phenology_data.csv` - 合并后的最终CSV文件

## 合并后的CSV文件结构

最终的CSV文件包含17列：

1. **Index** - 来源文件夹的索引号
2. **Name der Gewächse** - 植物名称
3-16. 14个物候观察阶段的日期列
17. **Genaue Bezeichnung der Standorte** - 观察地点描述

## 注意事项

- 程序会自动安装所需的Python包（odfpy和pandas）
- 确保输入目录路径正确
- 程序会跳过没有ODT文件的文件夹
- 只有16列的表格会被合并到最终文件中
- 2列的备注表格不会被包含在合并文件中

## 依赖项

- Python 3.x
- odfpy
- pandas（自动安装）