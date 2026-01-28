from flask import Flask, request, jsonify
from flask_cors import CORS
import sqlite3
import os
from datetime import datetime

app = Flask(__name__)
CORS(app)  # 启用跨域支持

# 数据库配置
DB_NAME = 'resource_audit.db'

def get_db_connection():
    """获取数据库连接"""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row  # 使返回结果为字典格式
    return conn

def init_db():
    """初始化数据库和表结构"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 创建 resources 表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS resources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            resource_name TEXT NOT NULL,
            channel TEXT NOT NULL,
            business_line TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(resource_name, channel, business_line)
        )
    ''')
    
    conn.commit()
    conn.close()

def init_sample_data():
    """初始化模拟数据"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 检查是否已有数据
    cursor.execute('SELECT COUNT(*) as count FROM resources')
    count = cursor.fetchone()['count']
    
    if count == 0:
        # A业务线（抖音_星光影院）的模拟数据
        sample_data_a = [
            ('白月光', '抖音', '星光影院'),
            ('狂飙', '抖音', '星光影院'),
            ('三体', '抖音', '星光影院'),
            ('流浪地球2', '抖音', '星光影院'),
            ('满江红', '抖音', '星光影院')
        ]
        
        # B业务线（快手_风行视频）的模拟数据
        sample_data_b = [
            ('白月光', '快手', '风行视频'),
            ('漫长的季节', '快手', '风行视频'),
            ('狂飙', '快手', '风行视频'),
            ('消失的她', '快手', '风行视频'),
            ('八角笼中', '快手', '风行视频')
        ]
        
        # 插入数据
        all_data = sample_data_a + sample_data_b
        cursor.executemany('''
            INSERT OR IGNORE INTO resources (resource_name, channel, business_line)
            VALUES (?, ?, ?)
        ''', all_data)
        
        conn.commit()
        print(f'已初始化 {len(all_data)} 条模拟数据')
    
    conn.close()

@app.route('/')
def index():
    """首页"""
    return jsonify({
        'message': '资源查重系统 API',
        'version': '1.0.0',
        'database': DB_NAME,
        'endpoints': {
            '/': 'API 信息',
            '/api/check': '检查资源是否重复',
            '/api/add': '添加新资源',
            '/api/resources': '获取所有资源列表',
            '/api/resource/<id>': '根据ID获取资源信息',
            '/api/resource/<id>': '根据ID删除资源',
            '/api/search': '搜索资源（支持按resource_name、channel、business_line搜索）',
            '/api/audit/import': '批量导入资源并对比（核心业务API）'
        }
    })

@app.route('/api/check', methods=['POST'])
def check_duplicate():
    """检查资源是否重复"""
    data = request.get_json()
    
    if not data:
        return jsonify({'error': '请求数据为空'}), 400
    
    resource_name = data.get('resource_name')
    channel = data.get('channel')
    business_line = data.get('business_line')
    
    if not all([resource_name, channel, business_line]):
        return jsonify({
            'error': '缺少必要参数',
            'required': ['resource_name', 'channel', 'business_line']
        }), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 检查是否重复（完全匹配）
    cursor.execute('''
        SELECT * FROM resources 
        WHERE resource_name = ? AND channel = ? AND business_line = ?
    ''', (resource_name, channel, business_line))
    
    exact_match = cursor.fetchone()
    
    # 检查同名资源（跨业务线或跨渠道）
    cursor.execute('''
        SELECT * FROM resources 
        WHERE resource_name = ?
    ''', (resource_name,))
    
    same_name_resources = cursor.fetchall()
    
    conn.close()
    
    is_duplicate = exact_match is not None
    
    result = {
        'duplicate': is_duplicate,
        'resource_name': resource_name,
        'channel': channel,
        'business_line': business_line
    }
    
    if is_duplicate:
        result['existing_resource'] = {
            'id': exact_match['id'],
            'resource_name': exact_match['resource_name'],
            'channel': exact_match['channel'],
            'business_line': exact_match['business_line'],
            'created_at': exact_match['created_at']
        }
    
    # 添加同名资源信息（用于展示跨业务线/跨渠道的情况）
    if same_name_resources:
        result['same_name_resources'] = [
            {
                'id': row['id'],
                'resource_name': row['resource_name'],
                'channel': row['channel'],
                'business_line': row['business_line'],
                'created_at': row['created_at']
            }
            for row in same_name_resources
        ]
    
    return jsonify(result), 200

@app.route('/api/add', methods=['POST'])
def add_resource():
    """添加新资源"""
    data = request.get_json()
    
    if not data:
        return jsonify({'error': '请求数据为空'}), 400
    
    resource_name = data.get('resource_name')
    channel = data.get('channel')
    business_line = data.get('business_line')
    
    if not all([resource_name, channel, business_line]):
        return jsonify({
            'error': '缺少必要参数',
            'required': ['resource_name', 'channel', 'business_line']
        }), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 检查是否重复
    cursor.execute('''
        SELECT * FROM resources 
        WHERE resource_name = ? AND channel = ? AND business_line = ?
    ''', (resource_name, channel, business_line))
    
    existing = cursor.fetchone()
    
    if existing:
        conn.close()
        return jsonify({
            'success': False,
            'duplicate': True,
            'message': '资源已存在',
            'existing_resource': {
                'id': existing['id'],
                'resource_name': existing['resource_name'],
                'channel': existing['channel'],
                'business_line': existing['business_line'],
                'created_at': existing['created_at']
            }
        }), 200
    
    # 插入新资源
    cursor.execute('''
        INSERT INTO resources (resource_name, channel, business_line)
        VALUES (?, ?, ?)
    ''', (resource_name, channel, business_line))
    
    conn.commit()
    resource_id = cursor.lastrowid
    
    # 获取刚插入的资源
    cursor.execute('SELECT * FROM resources WHERE id = ?', (resource_id,))
    new_resource = cursor.fetchone()
    
    conn.close()
    
    return jsonify({
        'success': True,
        'duplicate': False,
        'message': '资源添加成功',
        'resource': {
            'id': new_resource['id'],
            'resource_name': new_resource['resource_name'],
            'channel': new_resource['channel'],
            'business_line': new_resource['business_line'],
            'created_at': new_resource['created_at']
        }
    }), 201

@app.route('/api/resources', methods=['GET'])
def get_resources():
    """获取所有资源列表"""
    # 获取查询参数
    channel = request.args.get('channel')
    business_line = request.args.get('business_line')
    resource_name = request.args.get('resource_name')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 构建查询
    query = 'SELECT * FROM resources WHERE 1=1'
    params = []
    
    if channel:
        query += ' AND channel = ?'
        params.append(channel)
    
    if business_line:
        query += ' AND business_line = ?'
        params.append(business_line)
    
    if resource_name:
        query += ' AND resource_name LIKE ?'
        params.append(f'%{resource_name}%')
    
    query += ' ORDER BY created_at DESC'
    
    cursor.execute(query, params)
    resources = cursor.fetchall()
    
    conn.close()
    
    resources_list = [
        {
            'id': row['id'],
            'resource_name': row['resource_name'],
            'channel': row['channel'],
            'business_line': row['business_line'],
            'created_at': row['created_at']
        }
        for row in resources
    ]
    
    return jsonify({
        'total': len(resources_list),
        'resources': resources_list
    }), 200

@app.route('/api/resource/<int:resource_id>', methods=['GET'])
def get_resource_by_id(resource_id):
    """根据ID获取资源信息"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM resources WHERE id = ?', (resource_id,))
    resource = cursor.fetchone()
    
    conn.close()
    
    if not resource:
        return jsonify({'error': '资源不存在'}), 404
    
    return jsonify({
        'resource': {
            'id': resource['id'],
            'resource_name': resource['resource_name'],
            'channel': resource['channel'],
            'business_line': resource['business_line'],
            'created_at': resource['created_at']
        }
    }), 200

@app.route('/api/resource/<int:resource_id>', methods=['DELETE'])
def delete_resource(resource_id):
    """根据ID删除资源"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 先检查资源是否存在
    cursor.execute('SELECT * FROM resources WHERE id = ?', (resource_id,))
    resource = cursor.fetchone()
    
    if not resource:
        conn.close()
        return jsonify({'error': '资源不存在'}), 404
    
    # 删除资源
    cursor.execute('DELETE FROM resources WHERE id = ?', (resource_id,))
    conn.commit()
    conn.close()
    
    return jsonify({
        'success': True,
        'message': '资源已删除',
        'deleted_resource': {
            'id': resource['id'],
            'resource_name': resource['resource_name'],
            'channel': resource['channel'],
            'business_line': resource['business_line']
        }
    }), 200

@app.route('/api/search', methods=['GET'])
def search_resources():
    """搜索资源（支持多条件搜索）"""
    resource_name = request.args.get('resource_name', '')
    channel = request.args.get('channel', '')
    business_line = request.args.get('business_line', '')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 构建搜索查询
    conditions = []
    params = []
    
    if resource_name:
        conditions.append('resource_name LIKE ?')
        params.append(f'%{resource_name}%')
    
    if channel:
        conditions.append('channel LIKE ?')
        params.append(f'%{channel}%')
    
    if business_line:
        conditions.append('business_line LIKE ?')
        params.append(f'%{business_line}%')
    
    if not conditions:
        return jsonify({
            'error': '请至少提供一个搜索条件',
            'available_params': ['resource_name', 'channel', 'business_line']
        }), 400
    
    query = 'SELECT * FROM resources WHERE ' + ' AND '.join(conditions) + ' ORDER BY created_at DESC'
    cursor.execute(query, params)
    resources = cursor.fetchall()
    
    conn.close()
    
    resources_list = [
        {
            'id': row['id'],
            'resource_name': row['resource_name'],
            'channel': row['channel'],
            'business_line': row['business_line'],
            'created_at': row['created_at']
        }
        for row in resources
    ]
    
    return jsonify({
        'total': len(resources_list),
        'resources': resources_list
    }), 200

@app.route('/api/audit/import', methods=['POST'])
def audit_import():
    """批量导入资源并对比（核心业务API）
    
    接收JSON列表，格式：
    [
        {
            "resource_name": "资源名称",
            "channel": "频道",
            "business_line": "业务线"
        },
        ...
    ]
    
    对比逻辑：
    - 将导入的"资源名称+频道"与数据库对比
    - 如果已存在且业务线不同：标记为"重复资源"，显示原所属业务线
    - 如果不存在：标记为"独家资源"，所属业务线显示"未匹配"
    """
    data = request.get_json()
    
    if not data:
        return jsonify({'error': '请求数据为空'}), 400
    
    if not isinstance(data, list):
        return jsonify({
            'error': '请求数据格式错误',
            'expected': 'JSON数组格式',
            'example': [
                {
                    'resource_name': '资源名称',
                    'channel': '频道',
                    'business_line': '业务线'
                }
            ]
        }), 400
    
    if len(data) == 0:
        return jsonify({'error': '导入列表为空'}), 400
    
    # 验证每条数据的格式
    for idx, item in enumerate(data):
        if not isinstance(item, dict):
            return jsonify({
                'error': f'第 {idx + 1} 条数据格式错误，应为对象格式'
            }), 400
        
        required_fields = ['resource_name', 'channel']
        missing_fields = [field for field in required_fields if field not in item or not item[field]]
        
        if missing_fields:
            return jsonify({
                'error': f'第 {idx + 1} 条数据缺少必要字段',
                'missing_fields': missing_fields,
                'required': ['resource_name', 'channel']
            }), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    results = []
    
    for item in data:
        resource_name = item.get('resource_name')
        channel = item.get('channel')
        business_line = item.get('business_line', '')  # 业务线可选
        
        # 查询数据库中是否存在相同的"资源名称+频道"组合
        cursor.execute('''
            SELECT * FROM resources 
            WHERE resource_name = ? AND channel = ?
        ''', (resource_name, channel))
        
        existing_resources = cursor.fetchall()
        
        result_item = {
            'resource_name': resource_name,
            'channel': channel,
            'business_line': business_line if business_line else '未指定'
        }
        
        if existing_resources:
            # 如果存在，检查业务线是否不同
            existing_business_lines = [row['business_line'] for row in existing_resources]
            
            # 如果导入的业务线与已存在的业务线不同，标记为重复资源
            if business_line and business_line not in existing_business_lines:
                result_item['status'] = '重复资源'
                result_item['original_business_line'] = ', '.join(existing_business_lines)
                result_item['duplicate_count'] = len(existing_resources)
            elif business_line and business_line in existing_business_lines:
                # 完全匹配的情况
                result_item['status'] = '已存在'
                result_item['original_business_line'] = business_line
                result_item['duplicate_count'] = len(existing_resources)
            else:
                # 没有指定业务线，但存在相同资源名称+频道
                result_item['status'] = '重复资源'
                result_item['original_business_line'] = ', '.join(existing_business_lines)
                result_item['duplicate_count'] = len(existing_resources)
        else:
            # 不存在，标记为独家资源
            result_item['status'] = '独家资源'
            result_item['original_business_line'] = '未匹配'
            result_item['duplicate_count'] = 0
        
        results.append(result_item)
    
    conn.close()
    
    # 统计结果
    duplicate_count = sum(1 for r in results if r['status'] == '重复资源')
    exclusive_count = sum(1 for r in results if r['status'] == '独家资源')
    existing_count = sum(1 for r in results if r['status'] == '已存在')
    
    return jsonify({
        'total': len(results),
        'summary': {
            'duplicate': duplicate_count,
            'exclusive': exclusive_count,
            'existing': existing_count
        },
        'results': results
    }), 200

@app.errorhandler(500)
def internal_error(error):
    """服务器错误处理"""
    return jsonify({'error': '服务器内部错误'}), 500

# 初始化数据库和模拟数据
if __name__ == '__main__':
    init_db()
    init_sample_data()
    app.run(host='0.0.0.0', port=8080)
