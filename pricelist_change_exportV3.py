'''
版本更新說明：
V1: 
初始版本
V2: 
1.新增欄位名稱不同的狀況處理 
2.針對sheet新增或是刪除的情況做處理
V3:
1.新增log會存在輸出excel的第二個sheet中, 方便查詢
2.log增加更新/新增/刪除了幾個sheet
3.新增或是刪除的sheet的情況, 將不會抓header的列 
4.修正小數點比較的精度問題,將比較至小數點後兩位
V3.1:
1.增加處理欄位時先去掉換行符號(\n)的處理
2.增加Sales Team Pricelist的處理, 這個sheet沒有Description欄位, 故先將Order值拉進來, 最後再清空
3.修正Customer Support未正常輸出問題
4.修正新增商品價格會錯誤的問題   
  
'''

import tkinter as tk
from tkinter import filedialog, messagebox
import pandas as pd
import numpy as np
import time
import os
import threading

def get_col_map(sheet_name, df):
    # 先處理欄位名乾淨
    df.columns = df.columns.str.replace('\n', ' ').str.strip()
    # 特殊sheet判斷
    if sheet_name == 'Shenzhen Keju Tech':
        col_map = {'产品': 'Module', '产品简介': 'Description', '新价格': 'Price'}
    elif sheet_name == 'Customer Support':
        col_map = {'Module': 'Module', 'Product Description': 'Description', 'Price': 'Price'}
    elif sheet_name == 'Sales Team Pricelist':
        col_map = {'Module': 'Module', 'Order': 'Description', 'North America MAP': 'Price'}
    else:
        col_map = {'Module': 'Module', 'Description': 'Description', 'Price': 'Price'}
    return col_map


def run_compare(old_file, new_file, output_file, log_func=print):
    pricelist_file = new_file
    df_cost = pd.read_excel(pricelist_file, sheet_name='Cost Buildup', header=2)
    cost_lookup = pd.Series(df_cost['Header1'].values, index=df_cost['Module']).to_dict()
    df_cust = pd.read_excel(pricelist_file, sheet_name='Customer Master', header=0)
    df_cust.columns = df_cust.columns.str.strip()
    cust_lookup = pd.Series(df_cust['0.Regional'].values, index=df_cust['0.PRICE LIST']).to_dict()

    t0 = time.time()
    sheets_old_all = pd.read_excel(old_file, sheet_name=None, header=2)
    sheets_new_all = pd.read_excel(new_file, sheet_name=None, header=2)
    log_msgs = []
    def local_log(msg):
        log_func(msg)
        log_msgs.append(str(msg))

    local_log(f"全部 sheet 一次性讀取完畢，花費 {time.time() - t0:.2f} 秒")

    result_rows = []
    issues = []

    def check_issues(df, sheet_name, which):
        dup_rows = df[df['Module'].duplicated(keep=False)]
        for _, r in dup_rows.iterrows():
            issues.append({
                'Sheet': sheet_name,
                'Module': r['Module'],
                'Description': r['Description'],
                'Price': r['Price'],
                'Issue': f'Duplicate Module ({which})'
            })
        df = df.drop_duplicates(subset=['Module'], keep='first')

        mask_zero = (
            df['Module'].notna() & (df['Module'] != '') &
            df['Description'].notna() & (df['Description'] != '') &
            (df['Price'].isna() | (df['Price'] == 0) | (df['Price'] == ''))
        )
        for _, r in df[mask_zero].iterrows():
            issues.append({
                'Sheet': sheet_name,
                'Module': r['Module'],
                'Description': r['Description'],
                'Price': r['Price'],
                'Issue': f'Price is zero ({which})'
            })
        return df
    old_sheet_set = set(sheets_old_all.keys())
    new_sheet_set = set(sheets_new_all.keys())

    sheets_common = old_sheet_set & new_sheet_set
    sheets_only_in_new = new_sheet_set - old_sheet_set
    sheets_only_in_old = old_sheet_set - new_sheet_set

    n_update = 0
    n_added = 0
    n_deleted = 0

    t1 = time.time()
    for sheet_name in sheets_common:
        # 不處理的表放這裡以增加效率
        if sheet_name in ['MSRP', 'Cost Buildup', 'Customer Master', 'Amazon AM Master', 'Amazon AU Master', 'Amazon EUDI Master', 'Amazon JP Master']:
            local_log(f"跳過 sheet {sheet_name}（系統表）")
            continue
        local_log(f"開始處理 sheet: {sheet_name} ...")

        df_old = sheets_old_all[sheet_name]
        df_new = sheets_new_all[sheet_name]

        df_old = df_old.rename(columns=get_col_map(sheet_name, df_old))
        df_new = df_new.rename(columns=get_col_map(sheet_name, df_new))

        if not all(col in df_old.columns for col in ['Module', 'Description', 'Price']):
            local_log(f"  跳過 sheet {sheet_name}（缺欄位）")
            continue
        df_old = df_old[['Module', 'Description', 'Price']]
        df_new = df_new[['Module', 'Description', 'Price']]

        df_old = check_issues(df_old, sheet_name, 'Old')
        df_new = check_issues(df_new, sheet_name, 'New')

        df_old.set_index('Module', inplace=True)
        df_new.set_index('Module', inplace=True)
        df_merge = pd.merge(
            df_old, df_new,
            how='outer',
            left_index=True,
            right_index=True,
            suffixes=('_old', '_new'),
            indicator=True
        )

        updated = False
        # 這邊小數點比較要使用np.round來避免浮點數精度問題
        for idx, row in df_merge.iterrows():
            old_val = row.get('Price_old', None)
            new_val = row.get('Price_new', None)

            if (pd.isna(old_val) or old_val == '' or old_val == 0) and (pd.isna(new_val) or new_val == '' or new_val == 0):
                continue
            
            # 處理小數點比較精度問題
            try:
                old_val_f = np.round(float(old_val), 2) if not pd.isna(old_val) else None
                new_val_f = np.round(float(new_val), 2) if not pd.isna(new_val) else None
            except Exception:
                old_val_f = old_val
                new_val_f = new_val

            desc_change = None
            if row['_merge'] == 'right_only':
                desc_change = 'Added'
            elif row['_merge'] == 'left_only':
                desc_change = 'Deleted'
            elif row['_merge'] == 'both':
                if old_val_f != new_val_f:
                    desc_change = 'Updated'
                else:
                    continue
            else:
                continue

            updated = True

            try:
                change = np.round(float(new_val), 2) - np.round(float(old_val), 2) \
                    if not pd.isna(old_val) and not pd.isna(new_val) else None
                if change is not None:
                    change = np.round(change, 2)
            except Exception:
                change = None
            try:
                percentage_change = (
                    (np.round(float(new_val), 2) / np.round(float(old_val), 2)) - 1
                    if (not pd.isna(old_val) and not pd.isna(new_val) and np.round(float(old_val), 2) != 0)
                    else None
                )
                if percentage_change is not None:
                    percentage_change = np.round(percentage_change, 4)
            except Exception:
                percentage_change = None

            if desc_change == 'Added':
                trend = 'NEW'
            elif percentage_change is not None:
                trend = 'UP' if percentage_change > 0 else 'DOWN'
            else:
                trend = ''

            module = idx
            header1 = cost_lookup.get(module)
            region = cust_lookup.get(sheet_name)

            desc_val = row.get('Description_new') if not pd.isna(row.get('Description_new')) else row.get('Description_old')
            if sheet_name == 'Sales Team Pricelist':
                desc_val = ''

            result_rows.append({
                'PRICE_LIST': sheet_name,
                'ITEM': module,
                'DENSITY': desc_val,
                'PRICE_NEW': new_val_f,
                'PRICE_OLD': old_val_f,
                'PERCENTAGE_CHANGE': f"{percentage_change:.2%}" if percentage_change is not None else '',
                'TREND': trend,
                'PIRCE_DELTA': change,
                'REGION': region,
                'CATEGORY': header1,
                'Change Type': desc_change
            })

        if updated:
            n_update += 1
        local_log(f"  處理 sheet {sheet_name} 完成")

    for sheet_name in sheets_only_in_new:
        df_new = sheets_new_all[sheet_name]
        df_new = df_new.rename(columns=get_col_map(sheet_name, df_new))
        if not all(col in df_new.columns for col in ['Module', 'Description', 'Price']):
            continue
        df_new = df_new[['Module', 'Description', 'Price']]
        df_new = check_issues(df_new, sheet_name, 'New')
        region = cust_lookup.get(sheet_name, '')
        has_row = False
        for _, row in df_new.iterrows():
            price_new = row['Price']
            if pd.isna(price_new) or price_new == '' or price_new == 0:
                continue
            module = row['Module']
            if sheet_name == 'Sales Team Pricelist':
                # Sales Team Pricelist沒有Description欄位
                desc = ""
            else:
                desc = row['Description']
            category = cost_lookup.get(module, '')
            result_rows.append({
                'PRICE_LIST': sheet_name,
                'ITEM': module,
                'DENSITY': desc,
                'PRICE_NEW': np.round(float(price_new), 2),
                'PRICE_OLD': '',
                'PERCENTAGE_CHANGE': '',
                'TREND': 'NEW',
                'PIRCE_DELTA': '',
                'REGION': region,
                'CATEGORY': category,
                'Change Type': 'Added'
            })
            has_row = True
        if has_row:
            n_added += 1
        local_log(f"  新增 sheet {sheet_name} 完成")

    for sheet_name in sheets_only_in_old:
        df_old = sheets_old_all[sheet_name]
        df_old = df_old.rename(columns=get_col_map(sheet_name, df_old))
        if not all(col in df_old.columns for col in ['Module', 'Description', 'Price']):
            continue
        df_old = df_old[['Module', 'Description', 'Price']]
        df_old = check_issues(df_old, sheet_name, 'Old')
        region = cust_lookup.get(sheet_name, '')
        has_row = False
        for _, row in df_old.iterrows():
            price_old = row['Price']
            if pd.isna(price_old) or price_old == '' or price_old == 0:
                continue
            module = row['Module']
            if sheet_name == 'Sales Team Pricelist':
                # Sales Team Pricelist沒有Description欄位
                desc = ""
            else:
                desc = row['Description']
            category = cost_lookup.get(module, '')
            result_rows.append({
                'PRICE_LIST': sheet_name,
                'ITEM': module,
                'DENSITY': desc,
                'PRICE_NEW': '',
                'PRICE_OLD': np.round(float(price_old), 2),
                'PERCENTAGE_CHANGE': '',
                'TREND': '',
                'PIRCE_DELTA': '',
                'REGION': region,
                'CATEGORY': category,
                'Change Type': 'Deleted'
            })
            has_row = True
        if has_row:
            n_deleted += 1
        local_log(f"  刪除 sheet {sheet_name} 完成")

    local_log(f"  處理全部 sheet 完成，花費 {time.time() - t1:.2f} 秒")
    local_log(f"更新sheet數：{n_update}")
    local_log(f"新增sheet數：{n_added}")
    local_log(f"刪除sheet數：{n_deleted}")

    # 輸出報表與Log
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        pd.DataFrame(result_rows).to_excel(writer, index=False, sheet_name="Compare")
        # log另存一個Log分頁
        pd.DataFrame({'Log': log_msgs}).to_excel(writer, index=False, sheet_name="Log")
        if issues:
            pd.DataFrame(issues).to_excel(writer, index=False, sheet_name="Issues")

    local_log(f'全部完成！報告存在 {output_file}')

class PriceDiffGUI:
    def __init__(self, master):
        self.master = master
        master.title("Excel Pricelist Compare")
        self.default_output = os.path.join(os.getcwd(), 'pricelist_change_report.xlsx')

        self.old_label = tk.Label(master, text="Old Excel file:")
        self.old_label.grid(row=0, column=0, sticky="e")
        self.old_entry = tk.Entry(master, width=50)
        self.old_entry.grid(row=0, column=1)
        self.old_btn = tk.Button(master, text="Browse...", command=self.select_old)
        self.old_btn.grid(row=0, column=2)

        self.new_label = tk.Label(master, text="New Excel file:")
        self.new_label.grid(row=1, column=0, sticky="e")
        self.new_entry = tk.Entry(master, width=50)
        self.new_entry.grid(row=1, column=1)
        self.new_btn = tk.Button(master, text="Browse...", command=self.select_new)
        self.new_btn.grid(row=1, column=2)

        self.out_label = tk.Label(master, text="Output file (optional):")
        self.out_label.grid(row=2, column=0, sticky="e")
        self.out_entry = tk.Entry(master, width=50)
        self.out_entry.insert(0, self.default_output)
        self.out_entry.grid(row=2, column=1)
        self.out_btn = tk.Button(master, text="Save as...", command=self.select_output)
        self.out_btn.grid(row=2, column=2)

        self.run_btn = tk.Button(master, text="Run Comparison", command=self.run_compare)
        self.run_btn.grid(row=3, column=1)

        self.log_text = tk.Text(master, height=15, width=80)
        self.log_text.grid(row=4, column=0, columnspan=3, padx=10, pady=10)

    def log(self, msg):
        self.master.after(0, lambda: self._append_log(msg))

    def _append_log(self, msg):
        self.log_text.insert(tk.END, str(msg) + "\n")
        self.log_text.see(tk.END)

    def select_old(self):
        filename = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx;*.xls")])
        if filename:
            self.old_entry.delete(0, tk.END)
            self.old_entry.insert(0, filename)

    def select_new(self):
        filename = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx;*.xls")])
        if filename:
            self.new_entry.delete(0, tk.END)
            self.new_entry.insert(0, filename)

    def select_output(self):
        filename = filedialog.asksaveasfilename(defaultextension='.xlsx', filetypes=[("Excel files", "*.xlsx;*.xls")])
        if filename:
            self.out_entry.delete(0, tk.END)
            self.out_entry.insert(0, filename)

    def run_compare(self):
        self.log_text.delete(1.0, tk.END)
        self.log("Start!")
        old_file = self.old_entry.get()
        new_file = self.new_entry.get()
        output_file = self.out_entry.get() or self.default_output
        if not old_file or not new_file:
            messagebox.showerror("Input Error", "請選擇舊檔和新檔路徑")
            return
        threading.Thread(
            target=self._thread_run_compare,
            args=(old_file, new_file, output_file),
            daemon=True
        ).start()
        
    def _thread_run_compare(self, old_file, new_file, output_file):
        try:
            run_compare(old_file, new_file, output_file, log_func=self.log)
            self.log("Finished!")
            messagebox.showinfo("Done", f"比對完成，結果儲存在：\n{output_file}")
        except Exception as e:
            self.log(f"Error: {e}")
            messagebox.showerror("Error", f"執行失敗：\n{e}")

if __name__ == "__main__":
    root = tk.Tk()
    gui = PriceDiffGUI(root)
    root.mainloop()
