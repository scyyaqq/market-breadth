#!/usr/bin/env python3
import akshare as ak
import pandas as pd
import time
import random
from datetime import datetime
import os
import sys

# 8大板块映射
INDUSTRY_TO_MACRO = {
    "银行": "金融", "证券": "金融", "保险": "金融",
    "半导体": "科技", "软件开发": "科技", "计算机设备": "科技",
    "医药制造": "医药", "医疗服务": "医药", "生物制品": "医药",
    "电池": "新能源", "光伏设备": "新能源", "风电设备": "新能源",
    "白酒": "消费", "家电": "消费", "食品加工": "消费",
    "煤炭": "周期", "钢铁": "周期", "有色金属": "周期", "化工": "周期",
    "房地产开发": "地产", "房地产服务": "地产",
    "影视院线": "传媒", "游戏": "传媒", "广告营销": "传媒",
}

def get_market_breadth():
    """获取市场宽度数据"""
    print("开始获取数据...")
    
    try:
        # 获取行业板块列表
        board_df = ak.stock_board_industry_name_em()
        print(f"获取到{len(board_df)}个行业板块")
        time.sleep(random.uniform(2, 4))
        
        # 获取全市场实时行情
        spot_df = ak.stock_zh_a_spot_em()
        today = datetime.now().strftime("%Y-%m-%d")
        print(f"获取到{len(spot_df)}只股票行情")
        
        # 按行业统计
        rows = []
        for _, row in board_df.iterrows():
            industry = row['板块名称']
            if industry not in INDUSTRY_TO_MACRO:
                continue
                
            macro = INDUSTRY_TO_MACRO[industry]
            
            # 获取成分股
            try:
                cons_df = ak.stock_board_industry_cons_em(symbol=industry)
                time.sleep(random.uniform(1, 3))
                
                # 统计涨跌
                sub = spot_df[spot_df['代码'].isin(cons_df['代码'])]
                up = (sub['涨跌幅'] > 0).sum()
                down = (sub['涨跌幅'] < 0).sum()
                flat = (sub['涨跌幅'] == 0).sum()
                
                rows.append({
                    '日期': today,
                    '大板块': macro,
                    '细行业': industry,
                    '上涨家数': int(up),
                    '下跌家数': int(down),
                    '平盘家数': int(flat),
                })
                print(f"✓ {industry}: 涨{up} 跌{down}")
            except Exception as e:
                print(f"✗ {industry}失败: {e}")
                continue
        
        return pd.DataFrame(rows)
        
    except Exception as e:
        print(f"获取数据失败: {e}")
        # 创建空DataFrame
        return pd.DataFrame(columns=['日期', '大板块', '细行业', '上涨家数', '下跌家数', '平盘家数'])

if __name__ == "__main__":
    df = get_market_breadth()
    
    # 保存到output目录
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    
    filename = f"market_breadth_{datetime.now().strftime('%Y%m%d')}.csv"
    filepath = os.path.join(output_dir, filename)
    
    df.to_csv(filepath, index=False, encoding='utf-8-sig')
    print(f"\n保存成功: {filepath}")
    print(f"共{len(df)}条数据")
    
    # 打印汇总
    if len(df) > 0:
        print("\n各板块汇总:")
        summary = df.groupby('大板块')[[['上涨家数', '下跌家数']].sum()
        print(summary)
    else:
        print("警告: 没有获取到数据")
