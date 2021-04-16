import seaborn as sns
import matplotlib.pyplot as plt
import pymysql
import pandas as pd
# import seaborn as sns
from datetime import datetime,timedelta
def getMysqlDateFrame(sql,host,port,database,charset,user='dev',password='vKITJVGT7dianJMXDNERlcK2zYEbVkutEShK69SFDxTlIJF3SjLlHCbhZcfw'):
    '''
    host_name : l-db4-2,l-db1-5
    db : tablename ; Adx
    '''
    # password = 'vKITJVGT7dianJMXDNERlcK2zYEbVkutEShK69SFDxTlIJF3SjLlHCbhZcfw'
    # user = 'dev'
    # host =  host_name + '.prod.cn2.corp.agrant.cn'
    # port = 3306
    conn = pymysql.connect(host=host, port=port, user=user, passwd=password, db=database,charset='utf8')
    cur = conn.cursor()
    df = pd.read_sql_query(sql,conn)
    return df


monitor_mysql_config_korea = {
    "host": "l-db33-1.prod.qd10.corp.agrant.cn",
    "port": 3306,
    "user": "protect",
    "password": "zGaIAQNOfciZNQj3PlnpjjCp",
    "database": "CUE_MONITOR",
    "charset": "utf8"
}


sql_count = f"""select dateline,macid,count(1) as temp_count from CUE_INFRARED_ST_DATA.StShopFeverLog_202012 
                where dateline >= '2020-12-18' group by dateline,macid having temp_count >= 1000"""
df_111 = getMysqlDateFrame(sql_count,**monitor_mysql_config_korea)
macid_more_1000 = df_111["macid"].drop_duplicates().values
sql = f"""select macid,avggatertemp,alarmtemp,alarmtime,createtime from CUE_INFRARED_ST_DATA.StShopFeverLog_202012
          where createtime >= '2020-12-18 00:00:00' and macid in ('{"','".join(macid_more_1000)}');"""
df = getMysqlDateFrame(sql,**monitor_mysql_config_korea)
df["hour"] = df["createtime"].dt.hour
df["day"] = df["createtime"].dt.day
df["day_hour"] = df["day"].astype(str) +"_" + df["hour"].astype(str)
df["rank"] = df.groupby(["macid","day"])["alarmtime"].rank("first")


re=df[df['macid']=='1c:69:7a:61:80:0f']
print(re)
re.to_csv('./temp.csv')





def ChangeTempByMacid(df, macid):
    df_calcu = df[df["macid"] == macid]
    #当天总条数
    total_rank = df_calcu.shape[0]
    #初始化预警温度
    alarm_temp=37.5
    # 计算初始参数
    params_init = getParams(df_calcu[df_calcu["rank"] <= 1000],total_rank,alarm_temp)
    #计算人流量开始大于1000的hour
    start_hour=df_calcu[df_calcu["rank"] <= 1000]['hour'].astype('float').max()
    #print(params_init)

    # 每增加200个温度触发一次参数调整
    df_return = pd.DataFrame(columns=["macid", "avggatertemp", "alarmtemp", "alarmtime",
                                      "createtime", "hour", "day", "day_hour", "rank",
                                      "new_temp", "new_temp_1"])

    for idx in range(start_hour,24):
        alarm_temp=params_init[-1]
        df_init=getChangeTemp(df_calcu[df_calcu["hour"] == idx],params_init)
        params_init_res = getParams(df_calcu[df_calcu["hour"] ==idx],total_rank,alarm_temp)

        # 参数更新过程，如果出现分位数相等的情况，则认为分布过于集中，沿用一轮的参数
        #         print(len(set(params_init_res)))
        if (len(set(params_init_res)) < len(params_init)):
            params_init = params_init
        else:
            params_init = params_init_res
        #         print(params_init)
        df_return = pd.concat([df_return, df_init])
    return df_return


def getParams(df,total_rank,alarm_temp):
    list_percents = [i / 1000 for i in range(0, 900, 65)]
    list_percents.extend([0.9, 0.95, 1])
    df_percent_list = list(df[(df["avggatertemp"] > 26) & (df["avggatertemp"] < 36.5)].groupby(["macid"])[
                               "avggatertemp"].quantile(list_percents).reset_index()["avggatertemp"].values)

    num_997 = df[(df["avggatertemp"] > 26) & (df["avggatertemp"] < 42)].quantile(.997)["avggatertemp"]
    # if num_997 <= df_percent_list[-1]:
    #     num_997 = 37.2
    # df_percent_list.append(num_997)

    # 计算预警率
    alarm_rate = ((df['new_temp_1'].astype(float) > 37.5).sum()) / total_rank
    if alarm_rate < 1 / 1000 and alarm_rate > 5 / 1000:
        alarm_temp=(num_997-alarm_temp)/2+alarm_temp
    df_percent_list.append(alarm_temp)
    return df_percent_list


def getChangeTemp(df, df_percent_list):
    labels = [i / 10 + 35 for i in range(0, 16)]
    # 低温的bins
    bins = df_percent_list[:-1]
    #     print(len(labels))
    #     print(len(bins))
    # 用cut函数完成低温部分的计算
    df["new_temp"] = pd.cut(df["avggatertemp"], bins=bins, labels=labels).astype("float")
    #     print(df[df["new_temp"]==36.5])
    # 高温参数
    high_997 = df_percent_list[-1]
    df["new_temp_1"] = df.apply(fillnull, axis=1, args=(high_997,)).fillna(0)
    df["new_temp_1"] = df.apply(filltemp, axis=1).fillna(0)


    return df


def fillnull(row, high_997):
    # 将超低温填充为34.9
    if row["avggatertemp"] <= 26.5:
        return 34.9
    # 完成高温的相关操作
    elif row["avggatertemp"] >= 36.5:
        if row["avggatertemp"] < high_997:
            res_temp = getTargetPencent(row["avggatertemp"], 36.5, high_997, 36.5, 37.4)
        else:
            res_temp = getTargetPencent(row["avggatertemp"], high_997, 42, 37.5, 42)
        return res_temp
    else:
        return row['new_temp']


def filltemp(row):
    # 保证值进行升温，不进行降温
    if row["avggatertemp"] >= 42:
        return 0
    elif (row["avggatertemp"] < 36.5) & (row["avggatertemp"] >= row["new_temp_1"]):
        return row["avggatertemp"]
    else:
        return row["new_temp_1"]


def getTargetPencent(avgtemp, org_min, org_max, target_min, target_max):
    target_temp = ((avgtemp - org_min) / (org_max - org_min) * (target_max - target_min)) + target_min
    return target_temp



a=ChangeTempByMacid(df,'1c:69:7a:61:80:0f')
print(a)