import pymysql

test_db = pymysql.connect(
    user='wlsdud022',
    passwd='wlsdud022',
    host='192.168.134.193',
    db='SignalHouse',
    charset='utf8'
)
test_DB_cursor = test_db.cursor(pymysql.cursors.DictCursor)

create_sql = \
"""
create table operation_recorded (
	operation_id int primary key,
	filerecorded_ic int primary key,
    foreign key(operation_id) references operation(id),
    foreign key(filerecorded_ic) references filerecorded(id)
);
"""


