from flask import Flask, request, jsonify
from flask_cors import CORS
import os
from openpyxl import load_workbook

app = Flask(__name__)
CORS(app)

# 模拟已有的资源库：现在存储的是 (名称, 频道) 的元组
DATABASE_RESOURCES = {
    ("白月光", "电影频道"),
    ("狂飙", "电视剧频道"),
    ("流浪地球", "科幻频道"),
    ("半泽直树", "海外频道")
}

@app.route('/api/audit/upload', methods=['POST'])
def upload_audit():
    if 'file' not in request.files:
        return jsonify({"error": "未找到文件"}), 400
    
    file = request.files['file']
    
    try:
        wb = load_workbook(file)
        sheet = wb.active
        
        results = []
        # max_col=2 代表我们要读取前两列
        for row in sheet.iter_rows(min_row=1, max_col=2, values_only=True):
            # 解包：第一列是名称，第二列是频道
            name_val, channel_val = row
            
            if name_val is None: continue 
            
            name = str(name_val).strip()
            channel = str(channel_val).strip() if channel_val else "未知频道"
            
            # 双维度比对：只有名称和频道都对上才算重复
            is_dup = (name, channel) in DATABASE_RESOURCES
            
            results.append({
                "name": name,
                "channel": channel,
                "status": "重复资源" if is_dup else "新资源",
                "is_duplicate": is_dup
            })
        
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": f"解析失败: {str(e)}"}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)