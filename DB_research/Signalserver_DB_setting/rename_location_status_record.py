import pymysql
import pandas as pd

test_db = pymysql.connect(
    user='wlsdud022',
    passwd='wlsdud022',
    host='192.168.134.193',
    db='SignalHouse',
    charset='utf8'
)
test_DB_cursor = test_db.cursor(pymysql.cursors.DictCursor)

# create_sql = \
# """
# create table location_status_record
# (
#     id          int primary key auto_increment,
#     date        timestamp not null,
#     location_id int       not null,
#     status_id      int       not null,
#     foreign key (location_id) references location (id),
#     foreign key (status_id) references status (id),
#     index time_idx (date),
#     index status_idx (status_id)
# );
# """


