# 植物物候观测数据可视化平台

基于德国气象局(DWD)植物物候观测数据的交互式可视化平台，展示70年来德国境内植物生长周期的变化趋势。

## 功能特性

### 🏠 数据概览
- 实时统计展示：1700万+观测记录，1000+观测站点，97种植物
- 交互式地图预览德国境内观测站点分布
- 年度观测量趋势图表

### 🗺️ 地理分布分析
- 交互式地图显示所有观测站点
- 支持按州、海拔、观测频率筛选站点
- 地区统计图表和海拔分布分析
- 详细站点信息表格

### 📈 时间序列分析
- 物候期年际变化趋势分析
- 异常年份检测和气候影响评估
- 支持自定义物种和物候期组合分析
- 数据导出功能

### 🌿 物种研究
- 97种植物的详细信息展示
- 物种分组统计和物候特征分析
- 每个物种的地理分布和观测统计
- 网格和列表两种浏览模式

### 🛡️ 数据质量监控
- 三级质量等级分布统计
- 年度数据质量趋势分析
- 质量评分和改进建议

## 技术架构

### 后端
- **Flask**: Web框架
- **PostgreSQL**: 数据库
- **psycopg2**: 数据库连接器

### 前端
- **Bootstrap 5**: UI框架
- **Chart.js**: 图表可视化
- **Leaflet**: 地图可视化
- **jQuery**: JavaScript库

### 数据结构
数据库包含8个主要表：
- `dwd_observation`: 核心观测数据表（1700万+记录）
- `dwd_station`: 观测站点信息
- `dwd_species`: 植物物种信息
- `dwd_phase`: 物候期定义
- `dwd_quality_level`: 数据质量等级
- `dwd_quality_byte`: 质量字节码
- `dwd_species_group`: 物种分组
- `dwd_about`: 数据集元信息

## 安装部署

### 环境要求
- Python 3.8+
- PostgreSQL 12+
- 现代Web浏览器

### 1. 克隆项目
```bash
git clone <repository-url>
cd PhenoMapping
```

### 2. 安装依赖
```bash
pip install -r requirements.txt
```

### 3. 数据库配置
确保PostgreSQL服务正在运行，并且存在名为`pheno`的数据库，包含DWD物候观测数据。

在`app.py`中修改数据库配置：
```python
DB_CONFIG = {
    'host': 'localhost',
    'database': 'pheno', 
    'user': 'postgres',
    'password': 'your_password',
    'port': '5432'
}
```

### 4. 运行应用
```bash
python app.py
```

访问 `http://localhost:5000` 查看应用。

## API接口

### 主要API端点
- `GET /api/overview` - 数据概览统计
- `GET /api/stations` - 观测站点列表
- `GET /api/species` - 植物物种信息
- `GET /api/phases` - 物候期信息
- `GET /api/observations` - 观测数据（支持筛选）
- `GET /api/trends` - 趋势分析数据
- `GET /api/quality` - 数据质量统计

### 筛选参数
observations接口支持以下筛选参数：
- `station_id`: 站点ID
- `species_id`: 物种ID
- `phase_id`: 物候期ID
- `year_start`: 起始年份
- `year_end`: 结束年份
- `limit`: 返回记录数限制

## 数据来源

本项目使用德国气象局(DWD)公开的植物物候观测数据：
- **数据来源**: DWD Climate Data Center (CDC)
- **时间跨度**: 1953年至今
- **观测内容**: 植物发芽、开花、结果、落叶等物候期
- **空间覆盖**: 德国全境1000+观测站点

## 许可证

本项目遵循MIT许可证。数据来源于德国气象局，遵循其开放数据政策。

## 贡献

欢迎提交Issue和Pull Request来改进这个项目。

## 联系

如有问题或建议，请通过GitHub Issues联系。