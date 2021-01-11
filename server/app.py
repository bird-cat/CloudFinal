from flask import Flask, request
from flask_cors import CORS
from flask import jsonify
import pymysql
import random
from math import log
app = Flask(__name__)
CORS(app)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False


@app.route('/recommendary_problems')
def get_recommendary_problems():
    student_id = request.args.get('student_id')
    try:
        number = request.args.get('number')
        number = int(number)
    except:
        number = 10

    try:
        difficulties = request.args.get('difficulties').split(',')
    except Exception as e:
        difficulties = ['normal']

    difficulties = {difficulty.lower(): True for difficulty in difficulties}
    db = pymysql.connect("[Your DB HOST]",
                         "[Your DB USER","[Your DB PW]","cloud" )

    cursor = db.cursor()
    sql = f"SELECT content, accuracy  FROM `cloud`.`results` WHERE student_id = '{student_id}' ORDER BY rand()"
    cursor.execute(sql)
    data = cursor.fetchall()
    cnt = 0
    problems = {'easy': list(), 'normal': list(), 'hard': list()}
    for row in data:
        if cnt >= number:
            break
        content = row[0]
        accuracy = row[1]

        item = {'content': content, 'accuracy': accuracy}
        if accuracy >= 0.9 and 'easy' in difficulties:
            problems['easy'].append(item)
            cnt += 1
        elif accuracy < 0.75 and 'hard' in difficulties:
            problems['hard'].append(item)
            cnt += 1
        elif 0.75 <= accuracy < 0.9:
            if 'normal' in difficulties:
                problems['normal'].append(item)
                cnt += 1

    rtn_data = {
        'recommendary_problems': problems
    }
    db.close()
    return jsonify(rtn_data)

@app.route('/analytics')
def get_analytics():
    rtn_results = {'data': {}}
    db = pymysql.connect("", "", "","cloud" )

    total_acc = get_total_acc(db)
    stu_dist = get_stu_dist(db)
    solved_problem = get_solved_problem(db)
    problem_acc = get_problem_acc(db)
    diff_acc = get_diff_acc(db)
    active_stu = get_active_stu(db)
    stu_month = get_stu_month(db)

    ele_week = get_online_time(db, 'ele_week_csv')
    ele_weekend = get_online_time(db, 'ele_weekend_csv')
    jun_week = get_online_time(db, 'jun_week_csv')
    jun_weekend = get_online_time(db, 'jun_weekend_csv')
    sen_week = get_online_time(db, 'sen_week_csv')
    sen_weekend = get_online_time(db, 'sen_weekend_csv')

    rtn_results['name'] = ['Students Distribution', 'Solved Problem by Students',
                           'Accuracy of Each Problem', 'Accuracy of Different Difficulty',
                           'Active Students', 'Students Learning Status by Grade (2019-01)',
                           'Students Online Time [Elementary Weekday]',
                           'Students Online Time [Elementary Weekend]',
                           'Students Online Time [Junior Weekday]',
                           'Students Online Time [Junior Weekend]',
                           'Students Online Time [Senior Weekday]',
                           'Students Online Time [Senior Weekend]']
    rtn_results['total_acc'] = total_acc
    rtn_results['data']['stu_dist'] = stu_dist
    rtn_results['data']['solved_problem'] = solved_problem
    rtn_results['data']['problem_acc'] = problem_acc
    rtn_results['data']['diff_acc'] = diff_acc
    rtn_results['data']['active_stu'] = active_stu
    rtn_results['data']['stu_month'] = stu_month
    rtn_results['data']['ele_week'] = ele_week
    rtn_results['data']['ele_weekend'] = ele_weekend
    rtn_results['data']['jun_week'] = jun_week
    rtn_results['data']['jun_weekend'] = jun_weekend
    rtn_results['data']['sen_week'] = sen_week
    rtn_results['data']['sen_weekend'] = sen_weekend
    db.close()
    return jsonify(rtn_results)

def get_online_time(db, target):
    cursor = db.cursor()
    sql = f"select * from cloud.{target}"
    cursor.execute(sql)
    results = cursor.fetchall()

    rtn = {}
    rtn['columns'] = ['Online Time (Hour)', 'Students Count']
    rtn['rows'] = []
    for hour, stu_cnt in results:
        rtn['rows'].append(
                {
                    'Online Time (Hour)': hour,
                    'Students Count': stu_cnt
                }
            )
    
    return rtn

def get_stu_month(db):
    cursor = db.cursor()
    sql = "select user_grade, cnt from cloud.stu_log_csv slc  where timestamp_TW ='2019-01'"
    cursor.execute(sql)
    stu_month = cursor.fetchall()

    rtn = {}
    rtn['columns'] = ['User Grade', 'Students Count']
    rtn['rows'] = []
    for grade, stu_cnt in stu_month:
        rtn['rows'].append(
                {
                    'User Grade': grade,
                    'Students Count': stu_cnt
                }
            )
    
    return rtn

def get_ele_stu_time(db):
    cursor = db.cursor()
    sql = "select * from cloud.active_stu"
    cursor.execute(sql)
    ele_stu_time = cursor.fetchall()

    rtn = {}
    rtn['columns'] = ['Online Time (Hour)', 'Students Count']
    rtn['rows'] = []
    for o_t, stu_cnt in ele_stu_time:
        rtn['rows'].append(
                {
                    'Online Time (Hour)': o_t,
                    'Students Count': stu_cnt
                }
            )
    
    return rtn

def get_active_stu(db):
    cursor = db.cursor()
    sql = "select * from cloud.active_stu"
    cursor.execute(sql)
    active_stu = cursor.fetchall()

    rtn = {}
    rtn['columns'] = ['Active Number of Month', 'Students Count']
    rtn['rows'] = []
    for month_cnt, stu_cnt in active_stu:
        rtn['rows'].append(
                {
                    'Active Number of Month': month_cnt,
                    'Students Count': stu_cnt
                }
            )
    
    return rtn

def get_diff_acc(db):
    cursor = db.cursor()
    sql = "select * from cloud.accuracy_with_difficulty"
    cursor.execute(sql)
    diff_acc = cursor.fetchall()

    rtn = {}
    rtn['columns'] = ['Difficulty', 'Accuracy']
    rtn['rows'] = []
    for d, accuracy in diff_acc:
        rtn['rows'].append(
                {
                    'Difficulty': d,
                    'Accuracy': accuracy
                }
            )
    
    return rtn

def get_problem_acc(db):
    cursor = db.cursor()
    sql = "select * from cloud.problem_acc pa where accuracy != 1 limit 10"
    cursor.execute(sql)
    problem_acc = cursor.fetchall()

    rtn = {}
    rtn['columns'] = ['Problem Number', 'Accuracy']
    rtn['rows'] = []
    for p, accuracy in problem_acc:
        rtn['rows'].append(
                {
                    'Problem Number': p,
                    'Accuracy': accuracy
                }
            )
    
    return rtn

def get_total_acc(db):
    cursor = db.cursor()
    sql = "SELECT * from `cloud`.`total_acc_csv`"
    cursor.execute(sql)
    total_acc = cursor.fetchone()
    return round(total_acc[0], 5)

def get_stu_dist(db):
    cursor = db.cursor()
    sql = "SELECT * from `cloud`.`stu_grade_csv`"
    cursor.execute(sql)
    stu_dist = cursor.fetchall()

    rtn = {}
    rtn['columns'] = ['Grade', 'Students Count']
    rtn['rows'] = []
    for grade, cnt in stu_dist:
        rtn['rows'].append({
                'Grade': grade,
                'Students Count': cnt
            })

    return rtn

def get_solved_problem(db):
    rtn = {}
    rtn['columns'] = ['Indicator', 'Val']
    rtn['rows'] = []

    tmp = {'Max': 1702, 'Min': 1, 'Q1': 27,
           'Q3': 7, 'Std': 26.321, 'Avg': 22.16}

    for key, val in tmp.items():
        rtn['rows'].append(
                {
                    'Indicator': key,
                    'Val': val
                }
            )

    return rtn

@app.route('/comparison')
def get_comparsion():
    db = pymysql.connect("", "", "","cloud" )
    cursor = db.cursor()

    sql = "SELECT * from `cloud`.`comparison`"
    cursor.execute(sql)
    results = cursor.fetchall()
    db.close()

    mapping = {'accuracy_of_each_problem': 'Accuracy of Each Problem',
               'accuracy_with_difficulty': 'Accuracy of Different Difficulty',
               'active_stu': 'Active Students',
               'load_data': 'Load Raw Data',
               'stu_distribution': 'Students Distribution',
               'stu_learning_by_month': 'Students Learning Status by Month',
               'stu_online_time': 'Students Online Time',
               'stu_solved_problem': 'Solved Problem by Students',
               'total_accuracy': 'Total Accuracy'}

    order = {'accuracy_of_each_problem': 4,
               'accuracy_with_difficulty': 5,
               'active_stu': 6,
               'load_data': 0,
               'stu_distribution': 2,
               'stu_learning_by_month': 8,
               'stu_online_time': 7,
               'stu_solved_problem': 3,
               'total_accuracy': 1}

    rtn_results = {}
    rtn_results['name'] = [0] * len(mapping)
    rtn_results['speedup'] = [0] * len(mapping)
    rtn_results['data'] = [0] * len(mapping)

    for name, python_exec, spark_exec in results:
        rtn_results['name'][order[name]] = mapping[name]
        rtn_results['speedup'][order[name]] = round(python_exec / spark_exec, 2)
        rtn_results['data'][order[name]] = {
            'columns': ["Platform", "Execution Time"],
            'rows': [
                {
                    "Platform": 'Python Pandas',
                    "Execution Time": python_exec
                },
                {
                    "Platform": 'Spark',
                    "Execution Time": spark_exec
                }
            ]
        }
    return jsonify(rtn_results)

if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True, ssl_context=('server.crt', 'server.key'))
