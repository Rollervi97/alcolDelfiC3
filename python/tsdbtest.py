import psycopg2, os, timeit, json, sys, pandas as pd
from datetime import datetime, date

#
# def getTabInfo(tabname, cur):
#     comm = "\\d" + tabname
#     cur.execute(comm)
#
# def getUserInfo(cur):
#     cur.execute("\\du")
#
# def getTabsInfo(cur):
#     cur.execute("\\dt+")
#
# def getDBsInfo(cur):
#     cur.execute("\\l")
def getTab(tabname, fieldlist, fieldtype, notnulllist,PKindex):

    if not len(fieldlist) == len(fieldtype) or not len(fieldlist) == len(notnulllist) or PKindex<0 or PKindex>len(notnulllist):
        print("command not executed - lenght of required input not correct")
        return " "

    tabgenq = "CREATE TABLE public."+tabname + " ( "
    for i in range(len(fieldlist)):
        tabgenq += fieldlist[i]+" "+fieldtype[i]
        if notnulllist[i] == 1:
            tabgenq += " NOT NULL,"

        else:
            tabgenq += ","


    tabgenq = tabgenq[:-1]
    tabgenq += ");"
    return tabgenq



CONNECTION = "dbname=alcoldatabase user=alcol password=alcolpassword host=127.0.0.1 port=5432"
dbconn = psycopg2.connect(CONNECTION)
cur = dbconn.cursor()

gentab = "SELECT * FROM public.tab1 ORDER BY field1 ASC"

tn = "aletest1"

dl = ["send", "rec", "name", "msg"]
dt = ["integer", "integer", "text", "json"]
nll = [0, 0, 1, 1]
ind = 2
gentq = getTab(tn, dl, dt, nll, ind)

s = ""
s += "SELECT"
s += " table_schema"
s += ", table_name"
s += " FROM information_schema.tables"
s += " WHERE"
s += " ("
s += " table_schema = 'public'"
s += " )"
s += " ORDER BY table_schema, table_name;"

print(gentq)

try:
    # getDBsInfo(cur)
    # getUserInfo(cur)
    # getTabsInfo(cur)
    # getTabInfo("gen_tab", cur)
    # cur.execute(gentab)
    print('all info commands executed \n')
    # cur.execute(gentq)
    # dbconn.commit()
    print("Print each row and it's columns values")
    cur.execute(s)
    list_tables = cur.fetchall()

    for t_name_table in list_tables:
        print(t_name_table)
except (Exception, psycopg2.Error) as error:
    print(error.pgerror)



    #
    #
    # timenow = datetime.utcnow()
    # sid = 1
    # rid = 2
    # dataid = 3
    #
    #
    # cdir = "C:\\Users\\ASUS\\Desktop\\Lunar_Zebro_Code"
    # if not os.path.isdir(cdir+"\\json_msgs"):
    #     sys.exit("JSON message folder not found")
    #
    #
    # msglist = os.listdir(cdir+"/json_msgs")
    #
    # startt = timeit.default_timer()
    #
    # for i in range(len(msglist)):
    #     msgdir = cdir+"/json_msgs/"+msglist[i]
    #
    #
    #     msg = open(msgdir, "r")
    #     body = msg.read()
    #     body = body.replace("\'", "\"")
    #     jb = json.loads(body)
    #
    #
    #
    #     # l1 = body.split('{', -1)
    #     print(type(jb),jb["data"], "\n", jb["data"]["DhEmgpEqcf"])
    #     jbd = jb["data"]
    #     # insline = "INSERT INTO gen_tab (ts, sid, rid, dataid, datamsg) VALUES (%s, %d, %d, %d, %s);" (str(timenow), sid, rid, dataid, jb["data"])
    #     insline = "INSERT INTO gen_tab ( sid, rid, dataid, datamsg) VALUES ("
    #     insline += str(sid) + ", " + str(rid) + ", " +str(dataid) + ", " + json.dumps(jbd) + ");"
    #     print(insline)
    #     cur.execute(insline)
    #     print("sucecssful insert")
    #
    #
    #     break


# insert_data(dbconn)
# cur = dbconn.cursor()
#
# timescaledb quick tutorial
# query_create_newtable = "CREATE TABLE table_name (id SERIAL PRIMARY KEY, type VARCHAR(50), location VARCHAR(50))"
# # execute statement
# # NOTE: better to create a class with database infos and methods to create tables and other commands
# cur.execute(query_create_newtable)
# dbconn.commit()
# # cur.close()

