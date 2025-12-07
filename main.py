import pandas as pd
import os
import datetime

# --- 配置 ---
FILENAME = "data.csv"
URL = "https://datachart.500.com/dlt/history/newinc/history.php?limit=50" # 抓取最近50期

def update_data():
    print(f"[{datetime.datetime.now()}] 开始执行更新检查...")
    
    # 1. 读取现有的 CSV
    if os.path.exists(FILENAME):
        try:
            df_local = pd.read_csv(FILENAME)
            # 确保期号是数字类型
            df_local['期号'] = pd.to_numeric(df_local['期号'], errors='coerce')
            max_issue = df_local['期号'].max()
            print(f"本地最新期号: {max_issue}")
        except Exception as e:
            print(f"读取本地文件出错: {e}")
            return
    else:
        print("本地文件不存在，无法进行增量更新。")
        return

    # 2. 爬取最新数据
    try:
        tables = pd.read_html(URL)
        df_online = tables[0]
        
        # 简单清洗：只保留有数据的行
        df_online['期号'] = pd.to_numeric(df_online.iloc[:, 0], errors='coerce')
        df_online = df_online.dropna(subset=['期号'])
        
        # --- 关键：筛选出比本地更新的数据 ---
        new_data = df_online[df_online['期号'] > max_issue].copy()
        
        if not new_data.empty:
            print(f"发现 {len(new_data)} 期新数据！准备写入...")
            
            # --- 列名对齐 (非常重要) ---
            # 必须将新数据的列名改成和本地 data.csv 完全一样
            # 这里直接赋值本地文件的列名给新数据 (前提是列数量一致)
            if len(new_data.columns) == len(df_local.columns):
                new_data.columns = df_local.columns
            else:
                # 如果列数不对，强制只取前15列(根据你之前的csv结构)
                print("警告：列数不匹配，尝试截取前15列...")
                new_data = new_data.iloc[:, :15]
                new_data.columns = df_local.columns[:15]

            # 合并数据：新数据在最上面
            df_final = pd.concat([new_data, df_local], ignore_index=True)
            
            # 保存覆盖原文件
            df_final.to_csv(FILENAME, index=False, encoding='utf_8_sig')
            print("文件更新成功！")
        else:
            print("暂无新数据，无需更新。")
            
    except Exception as e:
        print(f"爬取或处理过程中出错: {e}")

if __name__ == "__main__":
    update_data()