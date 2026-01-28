from flask import Flask, request, jsonify
from flask_cors import CORS
import os

app = Flask(__name__)
CORS(app)

# 模拟数据库：这些是系统里已经有的资源
DATABASE_RESOURCES = {"白月光", "狂飙", "流浪地球", "半泽直树"}

@app.route('/api/audit/upload', methods=['POST'])
def upload_audit():
    if 'file' not in request.files:
        return jsonify({"error": "未找到文件"}), 400
    
    file = request.files['file']
    # 读取文件内容，按行拆分
    content = file.read().decode('utf-8')
    input_names = [line.strip() for line in content.split('\n') if line.strip()]

    results = []
    for name in input_names:
        # 只要在 DATABASE_RESOURCES 里的都算重复
        is_dup = name in DATABASE_RESOURCES
        results.append({
            "name": name,
            "status": "重复资源" if is_dup else "新资源",
            "is_duplicate": is_dup
        })
    
    return jsonify(results)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
