import os
import sqlite3
#import pymysql
from datetime import datetime
#from tqdm import tqdm
import psutil
import logging
import time
import argparse

# 设置日志记录
logging.basicConfig(filename='file_scan_log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


#################
# 连接到MySQL数据库
#db = pymysql.connect(
#    host="30.194.138.25",
#    user="user_monitor",
#    password="mezi#me",
#    database="test"
#)
#cursor = db.cursor()
################


# 连接到SQLite数据库
db = sqlite3.connect('file_scan_results.db')
cursor = db.cursor()

# 创建表
cursor.execute('''
    CREATE TABLE IF NOT EXISTS tablescan (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_name TEXT,
        file_path TEXT,
        file_extension TEXT,
        file_size INTEGER,
        file_modified_time TEXT,
        drive_letter TEXT,
        first_level_directory TEXT
    )
''')

cursor.execute('''
CREATE INDEX IF NOT EXISTS idx_file_extension ON tablescan(file_extension);
''')


# 准备插入MYSQL数据的SQL语句
#sql = "INSERT INTO disk_tablescan (file_name,file_path,file_extension,file_size,file_modified_time) VALUES (%s, %s, %s, %s, %s)"

def get_first_level_directory(full_path):
    # 初始化变量，以防路径只有一个层级
    first_level_directory = ""

    # 初始化一个空列表来存储路径的各个部分
    path_parts = []

    # 使用os.sep分割路径
    while os.path.dirname(full_path) != full_path:  # 当目录名不再变化时停止（即到达根目录或盘符）
        full_path, tail = os.path.split(full_path)
        if tail:
            path_parts.append(tail)

    # 如果path_parts列表有超过一个元素，获取第一层目录
    if len(path_parts) > 1:
        # 重构路径，包括盘符
        first_level_directory = os.path.join(full_path, path_parts[-1])

    return first_level_directory



def scan_files(directory,drive_letter):
    #读取日志中已扫描的文件路径
    scanned_files = set()
    if os.path.exists('file_scan_log.txt'):
        with open('file_scan_log.txt', 'r') as log_file:
            for line in log_file:
                scanned_files.add(line.strip())
    for root, dirs, files in os.walk(directory):
        try:
            for file in files:
                file_path = os.path.join(root, file)
                if file_path in scanned_files:
                    continue
                full_path = os.path.dirname(file_path)
                first_level_directory = get_first_level_directory(full_path)
 
                file_name = os.path.basename(file_path)
                file_extension = os.path.splitext(file_name)[1].lstrip(".")
                file_size = os.path.getsize(file_path)
                file_modified_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                formatted_time = file_modified_time.strftime('%Y-%m-%d %H:%M:%S')

                # 查询数据库中是否存在该文件
                #mysql version:cursor.execute("SELECT file_path FROM tablescan WHERE file_path = %s", (file_path,))
                cursor.execute("SELECT file_path FROM tablescan WHERE file_path = ?", (file_path,))
                result = cursor.fetchone()
                if result is None:
                    try:
                        # 执行插入数据的SQL语句
                        cursor.execute('''INSERT INTO tablescan (file_name, file_path, file_extension, file_size, file_modified_time,drive_letter,first_level_directory)VALUES (?, ?, ?, ?, ?,?,?)''', (file_name, file_path, file_extension, file_size, formatted_time,drive_letter,first_level_directory))

                        #mysql
                        #values = (file_name, file_path, file_extension, file_size, file_modified_time)
                        #cursor.execute(sql, values)
                        db.commit()
                        logging.info(file_path)
                        # 记录已扫描的文件路径
                        scanned_files.add(file_path)

                    except Exception as e:
                        print('22',f"Error: {e}")
                        logging.error(f"Error scanning file: {file_path} - {e}")
                        continue
        except Exception as e:
            print('22',f"Error: {e}")
            logging.error(f"Error scanning file: {file_path} - {e}")
            continue


# 指定要扫描的目录或盘符
#directory = "C:\\" #windows dir
#directory = "/data/apaul_project/"
# 获取系统中所有磁盘
#drives = [(drive.device, drive.mountpoint) for drive in psutil.disk_partitions()]
#drives = [('/dev/mapper/vg01-lv02', '/data')]
#print ('drive is:',drives)
#for drive in drives:
#    drive_letter, drive_path = drive
#    print ('xx',drive_path, drive_letter,type(drive_path),type(drive_letter))
#    scan_files(drive_path, drive_letter)








def main():
    # 记录程序开始时间
    start_time = time.time()
    start_time_str = datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')
    print(f"程序开始执行时间：{start_time_str}")

    parser = argparse.ArgumentParser()
    parser.add_argument('-d','--directory',required=True,type=str,default = None,help='windows盘符目录，可以是D:\也可以是D:\111目录')
    args = parser.parse_args()
    arg_directory=args.directory
    scan_files(arg_directory, arg_directory)
    # 关闭数据库连接
    cursor.close()
    db.close()


    # 记录程序结束时间
    end_time = time.time()
    end_time_str = datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')
    print(f"程序结束执行时间：{end_time_str}")
    # 计算并打印程序执行所花费的总时间
    elapsed_time = end_time - start_time
    print(f"程序总共运行了 {elapsed_time} 秒。")

if __name__ == '__main__':
     main()


