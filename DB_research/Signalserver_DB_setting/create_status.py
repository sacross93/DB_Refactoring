import pymysql

test_db = pymysql.connect(
    user='wlsdud022',
    passwd='wlsdud022',
    host='192.168.134.193',
    db='SignalHouse',
    charset='utf8'
)
test_DB_cursor = test_db.cursor(pymysql.cursors.DictCursor)


# create table status(
#     id int primary key auto_increment,
#     status varchar(16) not null
# );
#
# insert into status(status)
# values ('recording'),('ready'),('alert');