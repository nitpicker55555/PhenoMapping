#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HTML中文提取和替换工具
使用正则表达式从templates文件夹的HTML文件中提取中文文本，并替换为对应的英文
"""

import os
import re
import glob

# 中文到英文的翻译映射
TRANSLATIONS = {
    # base.html 中的中文
    "导航栏": "Navigation",
    "主要内容": "Main Content", 
    "页脚": "Footer",
    
    # index.html 中的中文
    "数据概览卡片": "Data Overview Cards",
    "加载中...": "Loading...",
    "数据源": "Data Source",
    "德国气象局": "German Weather Service (DWD)",
    "植物物候观测数据": "Plant Phenology Observation Data",
    "加载概览数据": "Load overview data",
    "初始化预览地图": "Initialize preview map",
    "加载年度趋势图表": "Load yearly trend chart",
    "格式化数字显示": "Format number display",
    "创建地图实例": "Create map instance",
    "聚焦德国": "Focus on Germany",
    "添加地图图层": "Add map layers",
    "加载站点数据": "Load station data",
    "只显示前50个观测数量最多的站点": "Only display top 50 stations with most observations",
    "这里暂时使用模拟数据，实际应从API获取": "Using simulated data here, should actually get from API",
    "生成近20年的模拟趋势数据": "Generate simulated trend data for recent 20 years",
    "记录数量": "Record Count",
    "年份": "Year",
    
    # geography.html 中的中文
    "筛选控件": "Filter Controls",
    "地图和统计": "Map and Statistics",
    "数据表格": "Data Table",
    "筛选按钮事件": "Filter button event",
    "创建主地图": "Create main map",
    "默认地形图层": "Default terrain layer",
    "卫星图层": "Satellite layer",
    "图层切换": "Layer switching",
    "添加标记图层组": "Add marker layer group",
    "填充州筛选器": "Populate state filter",
    "显示站点": "Display stations",
    "更新统计图表": "Update statistical charts",
    "更新数据表格": "Update data table",
    "清除现有标记": "Clear existing markers",
    "根据观测数量确定标记大小和颜色": "Determine marker size and color based on observation count",
    "地区分布统计": "Regional distribution statistics",
    "创建或更新地区分布图表": "Create or update regional distribution chart",
    "海拔分布直方图": "Altitude distribution histogram",
    "显示前100个站点": "Show first 100 stations",
    "性能考虑": "Performance consideration",
    
    # quality.html 中的中文
    "数据质量": "Data Quality",
    "植物物候观测数据可视化平台": "Plant Phenology Observation Data Visualization Platform",
    "数据质量监控": "Data Quality Monitoring",
    "监控和分析物候观测数据的质量状况": "Monitor and analyze the quality status of phenological observation data",
    "质量概览卡片": "Quality overview cards",
    "高质量数据": "High Quality Data",
    "已完成例行检查和修正": "Routine inspection and correction completed",
    "中等质量数据": "Medium Quality Data",  
    "已检查但未修正": "Inspected but not corrected",
    "基础数据": "Basic Data",
    "仅完成形式检查": "Only formal inspection completed",
    "质量分析图表": "Quality analysis charts",
    "质量等级分布": "Quality level distribution",
    "年度质量趋势": "Annual quality trends",
    "质量详细分析": "Detailed quality analysis", 
    "年度质量详细分布": "Annual detailed quality distribution",
    "正在分析数据质量...": "Analyzing data quality...",
    "质量等级说明和数据表": "Quality level description and data table",
    "质量问题分析": "Quality issue analysis",
    "质量等级说明": "Quality level description",
    "在ROUTINE中检查，完成例行修正处理的数据": "Data checked in ROUTINE and completed routine correction processing",
    "在ROUTINE中检查，但未进行修正的数据": "Data checked in ROUTINE but not corrected",
    "仅完成解码和加载时的形式检查": "Only completed formal inspection during decoding and loading",
    "质量建议": "Quality Recommendations",
    "加载质量数据失败": "Failed to load quality data",
    "质量": "Quality",
    "其他": "Other",
    "数据质量优秀": "Excellent data quality",
    "数据可靠性很高": "Data reliability is very high",
    "数据质量良好": "Good data quality",
    "总体质量不错": "Overall quality is good",
    "建议：继续优化数据处理流程": "Recommendation: Continue optimizing data processing workflows",
    "需要关注": "Needs attention",
    "建议：加强数据质量控制和后处理": "Recommendation: Strengthen data quality control and post-processing",
    "数据分布": "Data distribution",
    
    # species.html 中的中文
    "物种": "Species",
    "德文": "German",
    "拉丁名": "Latin Name",
    "没有找到符合条件的物种": "No species found matching the criteria",
    "物种名称": "Species Name",
    "英文：": "English: ",
    "德文：": "German: ",
    "拉丁名：": "Latin Name: ",
    "分类信息": "Classification Information",
    "未分组": "Ungrouped",
    "观测概况": "Observation Overview",
    "条记录": "records",
    "加载物种详细数据失败": "Failed to load species detailed data",
    "观测次数": "Observation Count",
    "观测年份": "Observation Years",
    "时间跨度": "Time Span",
    "物候期数": "Number of Phenological Phases",
    "观测站点": "Observation Stations",
    "无数据": "No data",
    "年": "years",
    
    # timeline.html 中的中文
    "分析参数设置": "Analysis Parameter Settings",
    "请选择物种...": "Please select species...",
    "请选择物候期...": "Please select phenological phase...",
    "开始年份": "Start Year",
    "结束年份": "End Year",
    "开始分析": "Start Analysis",
    "分析结果": "Analysis Results",
    "物候期年际趋势": "Phenological Period Interannual Trends",
    "分析概要": "Analysis Summary",
    "物候日历": "Phenological Calendar",
    "统计分析": "Statistical Analysis",
    "年度分布": "Annual Distribution",
    "观测数量统计": "Observation Count Statistics",
    "异常年份检测": "Anomaly Year Detection",
    "详细观测数据": "Detailed Observation Data",
    "导出数据": "Export Data",
    "年份": "Year",
    "站点": "Station",
    "日期": "Date",
    "年积日": "Day of Year",
    "物候期": "Phenological Phase",
    "通过JavaScript填充数据": "Data will be populated through JavaScript",
    "分析中...": "Analyzing...",
    "分析失败，请重试": "Analysis failed, please try again",
    "未找到符合条件的数据": "No data found matching the criteria",
    "年积日（天）": "Day of Year (days)",
    "大约": "Approximately",
    "数据概览": "Data Overview",
    "观测记录": "Observation Records",
    "年份范围": "Year Range",
    "平均物候期": "Average Phenological Period",
    "极值分析": "Extreme Value Analysis",
    "最早年份": "Earliest Year",
    "最晚年份": "Latest Year",
    "跨度": "Time Span",
    "天": "days",
    "趋势分析": "Trend Analysis",
    "延迟趋势": "Delayed Trend",
    "提前趋势": "Advanced Trend",
    "延迟": "Delayed",
    "提前": "Advanced",
    "大约每10年": "approximately per 10 years",
    "最近10年": "Last 10 Years",
    "早期数据": "Earlier Data",
    "未检测到显著异常年份": "No significant anomaly years detected",
    "天数": "days",
    "无详细观测数据": "No detailed observation data available",
    "未知": "Unknown",
    "显示前500条记录...": "Showing first 500 records...",
    "没有可导出的数据": "No data available for export",
    "加载物种数据失败": "Failed to load species data",
    "加载物候期数据失败": "Failed to load phenological phase data",
    "请选择物种和物候期": "Please select species and phenological phase",
    "加载详细观测数据失败": "Failed to load detailed observation data",
}

def extract_chinese_text(text):
    """使用正则表达式提取文本中的中文"""
    # 匹配中文字符（包括中文标点符号）
    chinese_pattern = r'[\u4e00-\u9fff\u3000-\u303f\uff00-\uffef]+'
    chinese_matches = re.findall(chinese_pattern, text)
    return chinese_matches

def replace_chinese_in_text(text, translations):
    """在文本中替换中文为英文"""
    result_text = text
    chinese_found = []
    
    # 按长度排序，优先替换长文本（避免部分匹配问题）
    sorted_translations = sorted(translations.items(), key=lambda x: len(x[0]), reverse=True)
    
    for chinese, english in sorted_translations:
        if chinese in result_text:
            chinese_found.append(chinese)
            result_text = result_text.replace(chinese, english)
    
    return result_text, chinese_found

def process_html_file(file_path, translations):
    """处理单个HTML文件"""
    print(f"\n处理文件: {file_path}")
    
    # 读取文件内容
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 提取所有中文文本
    chinese_texts = extract_chinese_text(content)
    if chinese_texts:
        print(f"发现中文文本: {set(chinese_texts)}")
    else:
        print("未发现中文文本")
        return
    
    # 替换中文为英文
    new_content, replaced_texts = replace_chinese_in_text(content, translations)
    
    if replaced_texts:
        print(f"已替换的中文: {replaced_texts}")
        
        # 创建备份文件
        backup_path = file_path + '.backup'
        with open(backup_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"原文件已备份到: {backup_path}")
        
        # 写入新内容
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"文件已更新: {file_path}")
    else:
        print("没有匹配的翻译，文件未修改")

def main():
    """主函数"""
    # 获取templates目录下的所有HTML文件
    templates_dir = "templates"
    html_files = glob.glob(os.path.join(templates_dir, "*.html"))
    
    print("HTML中文提取和替换工具")
    print("=" * 50)
    print(f"在目录 {templates_dir} 中找到 {len(html_files)} 个HTML文件")
    
    # 处理每个文件
    for file_path in html_files:
        process_html_file(file_path, TRANSLATIONS)
    
    print("\n" + "=" * 50)
    print("处理完成！")
    
    # 显示所有可用的翻译
    print(f"\n翻译映射表包含 {len(TRANSLATIONS)} 个条目:")
    for chinese, english in TRANSLATIONS.items():
        print(f"  {chinese} -> {english}")

if __name__ == "__main__":
    main()