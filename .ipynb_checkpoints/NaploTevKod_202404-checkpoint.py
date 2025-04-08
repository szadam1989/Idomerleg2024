#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import os
import oracledb
import getpass
import numpy as np


# In[2]:


def makeInsert(number):
    valuesText = ""
    for row in range(number):
        valuesText = valuesText + ":" + str(row + 1) + ","

    valuesText = valuesText[:-1]

    return valuesText


# In[3]:


pd.options.mode.chained_assignment = None

username = getpass.getuser()
password = "Masodikpozicio1989" #getpass.getpass(f"Kérlek, add meg a(z) {username} felhasználói nevedhez tartozó jelszót: ")


# In[4]:


database = oracledb.makedsn(host = "emerald.ksh.hu", port = "1521", service_name = "emerald.ksh.hu")
conn = oracledb.connect(user = username, password = password, dsn = database)
cur = conn.cursor()


# In[5]:


idomerleg_2nev = pd.read_excel(io = os.path.abspath(os.getcwd() + "\Excel_Files\YS_1711_KERDOIVKOD_V2407_V00_VEGLEGES.xlsx"), sheet_name = "Munka1", header = 0)
print(f"Az Időmérleg adatgyűjtés második negyedévi adatait tartalmazó adatkeret sor- és oszlopszámai : {idomerleg_2nev.shape}")


# In[6]:


pd.set_option('display.precision', 0)
#idomerleg_2nev["letrehozva"] = idomerleg_2nev["letrehozva"].astype("datetime64[ns]")
idomerleg_2nev = idomerleg_2nev.replace({pd.NaT: None}).replace({"NaT": None}).replace({np.NaN: None})

#idomerleg_2nev['ISZAK'] = str(idomerleg_2nev['ISZAK'])
#idomerleg_2nev['ISZAK'] = idomerleg_2nev['ISZAK'].apply(lambda x: x.split('.')[0])
#idomerleg_2nev.ISZAK = idomerleg_2nev.ISZAK.str.rjust(2, '0')

#idomerleg_2nev['FIBB202'] = idomerleg_2nev['FIBB202'].apply(lambda x: x.split('.')[0])
#idomerleg_2nev.FIBB202 = idomerleg_2nev.FIBB202.str.rjust(2, '0')

'''idomerleg_2nev['FIBD003'] = str(idomerleg_2nev['FIBD003'])
idomerleg_2nev['FIBD003'] = idomerleg_2nev['FIBD003'].apply(lambda x: x.split('.')[0])
idomerleg_2nev.FIBD003 = idomerleg_2nev.FIBD003.str.rjust(2, '0')

idomerleg_2nev['FIBD004'] = str(idomerleg_2nev['FIBD004'])
idomerleg_2nev['FIBD004'] = idomerleg_2nev['FIBD004'].apply(lambda x: x.split('.')[0])
idomerleg_2nev.FIBD004 = idomerleg_2nev.FIBD004.str.rjust(2, '0')

idomerleg_2nev['FIBD027'] = str(idomerleg_2nev['FIBD027'])
idomerleg_2nev['FIBD027'] = idomerleg_2nev['FIBD027'].apply(lambda x: x.split('.')[0])
idomerleg_2nev.FIBD027 = idomerleg_2nev.FIBD027.str.rjust(2, '0')

idomerleg_2nev['FIBD028'] = str(idomerleg_2nev['FIBD028'])
idomerleg_2nev['FIBD028'] = idomerleg_2nev['FIBD028'].apply(lambda x: x.split('.')[0])
idomerleg_2nev.FIBD028 = idomerleg_2nev.FIBD028.str.rjust(2, '0')

idomerleg_2nev['FIBD102'] = str(idomerleg_2nev['FIBD102'])
idomerleg_2nev['FIBD102'] = idomerleg_2nev['FIBD102'].apply(lambda x: x.split('.')[0])
idomerleg_2nev.FIBD102 = idomerleg_2nev.FIBD102.str.rjust(2, '0')

idomerleg_2nev['FIBD103'] = str(idomerleg_2nev['FIBD103'])
idomerleg_2nev['FIBD103'] = idomerleg_2nev['FIBD103'].apply(lambda x: x.split('.')[0])
idomerleg_2nev.FIBD103 = idomerleg_2nev.FIBD103.str.rjust(2, '0')

idomerleg_2nev['FIBD107'] = str(idomerleg_2nev['FIBD107'])
idomerleg_2nev['FIBD107'] = idomerleg_2nev['FIBD107'].apply(lambda x: x.split('.')[0])
idomerleg_2nev.FIBB107 = idomerleg_2nev.FIBB107.str.rjust(4, '0')
print(idomerleg_2nev)'''



#print(idomerleg_2nev['ISZAK'])
#print(idomerleg_2nev['FIBD027'])


# In[7]:


values = makeInsert(246)#246

outputName = "FI.YS_1711_KERDOIVKOD_V2407_V00"
attributesForInsert = """KODOL_SZAM, TEV, ISZAK, OSAP_REG, LAKAZON, HSOR, FIBD001, FIBD002, FIBD003, 
FIBD004, FIBD026, FIBD027, FIBD028, OSAP_SZEMGYERHAZT, FIBB001, FIBB002, FIBB003, FIBB004, FIBB005, FIBB006, 
FIBB007, FIBB008, FIBB009, FIBB010, FIBB011, FIBB012, FIBB013, FIBB014, FIBB015, FIBB016, FIBB017, FIBB018, 
FIBB023, FIBB101, FIBB102, FIBB103, FIBB104, FIBB105, FIBB106, FIBB107, FIBB107SZOV, FIBB108, FIBB109, 
FIBB109SZOV, FOGLALKKOD, IND5, FOGLALKNEV, FEOR1, FEOR2, FEOR3, TELSZOVKOD, TELSZOVNEV, TEAOR1, TEAOR2, TEAOR3, 
FIBB111, FIBB112, FIBB117, FIBB119, FIBB120, FIBB121, FIBB123, FIBB124, FIBB128, FIBB129, FIBB201, FIBB202, 
FIBB203, FIBB204, FIBB205, FIBB206, FIBB207, FIBB208, FIBB209, FIBB210, FIBB211, ISZAKV, FIBB212, FIBB213, 
FIBB214, FIBB215, FIBB226, FIBB227, FIBB228, FIBB231, FIBB239, FIBB240, FIBB249, FIBB251, FIBB413, FIBB601, 
FIBB603, FIBB605, FIBB607, FIBB711, FIBB712, FIBB713, FIBB714, FIBB024, FIBB025, FIBB026, FIBB027, FIBB034, 
FIBB035, FIBB036, FIBB037, FIBB038, FIBB039, FIBB261, FIBB725, FIBB726, FIBB727, FIBB728, FIBD101, FIBD102, 
FIBD103, FIBD107, FIBA001, FIBA002, FIBA006, FIBA007, FIBA008, FIBA009, FIBA101_0, FIBA102_0, FIBA103_0, 
FIBA104_0, FIBA105_0, FIBA106_0, FIBA101_1, FIBA102_1, FIBA103_1, FIBA104_1, FIBA105_1, FIBA106_1, FIBA101_2, 
FIBA102_2, FIBA103_2, FIBA104_2, FIBA105_2, FIBA106_2, FIBA101_3, FIBA102_3, FIBA103_3, FIBA104_3, FIBA105_3, 
FIBA106_3, FIBA101_4, FIBA102_4, FIBA103_4, FIBA104_4, FIBA105_4, FIBA106_4, FIBA101_5, FIBA102_5, FIBA103_5, 
FIBA104_5, FIBA105_5, FIBA106_5, FIBA101_6, FIBA102_6, FIBA103_6, FIBA104_6, FIBA105_6, FIBA106_6, FIBA101_7, 
FIBA102_7, FIBA103_7, FIBA104_7, FIBA105_7, FIBA106_7, FIBA101_8, FIBA102_8, FIBA103_8, FIBA104_8, FIBA105_8, 
FIBA106_8, FIBA101_9, FIBA102_9, FIBA103_9, FIBA104_9, FIBA105_9, FIBA106_9, FIBA101_10, FIBA102_10, FIBA103_10, 
FIBA104_10, FIBA105_10, FIBA106_10, FIBA101_11, FIBA102_11, FIBA103_11, FIBA104_11, FIBA105_11, FIBA106_11, 
FIBA101_12, FIBA102_12, FIBA103_12, FIBA104_12, FIBA105_12, FIBA106_12, FI09_1, FIBA401_1, FIBA402_1, FIBA403_1, 
FI09_2, FIBA401_2, FIBA402_2, FIBA403_2, FI09_3, FIBA401_3, FIBA402_3, FIBA403_3, FI09_4, FIBA401_4, FIBA402_4, 
FIBA403_4, FI09_5, FIBA401_5, FIBA402_5, FIBA403_5, FI09_6, FIBA401_6, FIBA402_6, FIBA403_6, FI09_7, FIBA401_7, 
FIBA402_7, FIBA403_7, FI09_8, FIBA401_8, FIBA402_8, FIBA403_8, FI09_9, FIBA401_9, FIBA402_9, FIBA403_9, FIBC201, 
ISZAKV_VK, FOGLALKKOD_1NE, FOGLALKKOD_2NE, FOGLALKKOD_3NE, FOGLALKNEV_1NE, FOGLALKNEV_2NE, FOGLALKNEV_3NE, 
FIBC220""" 



output_insert_sql = "INSERT INTO " + outputName + "(" + attributesForInsert + ") VALUES(" + values + ")"
cur.executemany(output_insert_sql, idomerleg_2nev[["KODOL_SZAM", "TEV", "ISZAK", "OSAP_REG", "LAKAZON", "HSOR", "FIBD001", "FIBD002", "FIBD003", 
"FIBD004", "FIBD026", "FIBD027", "FIBD028", "OSAP_SZEMGYERHAZT", "FIBB001", "FIBB002", "FIBB003", "FIBB004", "FIBB005", "FIBB006",
"FIBB007", "FIBB008", "FIBB009", "FIBB010", "FIBB011", "FIBB012", "FIBB013", "FIBB014", "FIBB015", "FIBB016", "FIBB017", "FIBB018",
"FIBB023", "FIBB101", "FIBB102", "FIBB103", "FIBB104", "FIBB105", "FIBB106", "FIBB107", "FIBB107SZOV", "FIBB108", "FIBB109",
"FIBB109SZOV", "FOGLALKKOD", "IND5", "FOGLALKNEV", "FEOR1", "FEOR2", "FEOR3", "TELSZOVKOD", "TELSZOVNEV", "TEAOR1", "TEAOR2", "TEAOR3",
"FIBB111", "FIBB112", "FIBB117", "FIBB119", "FIBB120", "FIBB121", "FIBB123", "FIBB124", "FIBB128", "FIBB129", "FIBB201", "FIBB202",
"FIBB203", "FIBB204", "FIBB205", "FIBB206", "FIBB207", "FIBB208", "FIBB209", "FIBB210", "FIBB211", "ISZAKV", "FIBB212", "FIBB213", 
"FIBB214", "FIBB215", "FIBB226", "FIBB227", "FIBB228", "FIBB231", "FIBB239", "FIBB240", "FIBB249", "FIBB251", "FIBB413", "FIBB601",
"FIBB603", "FIBB605", "FIBB607", "FIBB711", "FIBB712", "FIBB713", "FIBB714", "FIBB024", "FIBB025", "FIBB026", "FIBB027", "FIBB034",
"FIBB035", "FIBB036", "FIBB037", "FIBB038", "FIBB039", "FIBB261", "FIBB725", "FIBB726", "FIBB727", "FIBB728", "FIBD101", "FIBD102",
"FIBD103", "FIBD107", "FIBA001", "FIBA002", "FIBA006", "FIBA007", "FIBA008", "FIBA009", "FIBA101_0", "FIBA102_0", "FIBA103_0",
"FIBA104_0", "FIBA105_0", "FIBA106_0", "FIBA101_1", "FIBA102_1", "FIBA103_1", "FIBA104_1", "FIBA105_1", "FIBA106_1", "FIBA101_2",
"FIBA102_2", "FIBA103_2", "FIBA104_2", "FIBA105_2", "FIBA106_2", "FIBA101_3", "FIBA102_3", "FIBA103_3", "FIBA104_3", "FIBA105_3",
"FIBA106_3", "FIBA101_4", "FIBA102_4", "FIBA103_4", "FIBA104_4", "FIBA105_4", "FIBA106_4", "FIBA101_5", "FIBA102_5", "FIBA103_5",
"FIBA104_5", "FIBA105_5", "FIBA106_5", "FIBA101_6", "FIBA102_6", "FIBA103_6", "FIBA104_6", "FIBA105_6", "FIBA106_6", "FIBA101_7",
"FIBA102_7", "FIBA103_7", "FIBA104_7", "FIBA105_7", "FIBA106_7", "FIBA101_8", "FIBA102_8", "FIBA103_8", "FIBA104_8", "FIBA105_8",
"FIBA106_8", "FIBA101_9", "FIBA102_9", "FIBA103_9", "FIBA104_9", "FIBA105_9", "FIBA106_9", "FIBA101_10", "FIBA102_10", "FIBA103_10",
"FIBA104_10", "FIBA105_10", "FIBA106_10", "FIBA101_11", "FIBA102_11", "FIBA103_11", "FIBA104_11", "FIBA105_11", "FIBA106_11",
"FIBA101_12", "FIBA102_12", "FIBA103_12", "FIBA104_12", "FIBA105_12", "FIBA106_12", "FI09_1", "FIBA401_1", "FIBA402_1", "FIBA403_1",
"FI09_2", "FIBA401_2", "FIBA402_2", "FIBA403_2", "FI09_3", "FIBA401_3", "FIBA402_3", "FIBA403_3", "FI09_4", "FIBA401_4", "FIBA402_4",
"FIBA403_4", "FI09_5", "FIBA401_5", "FIBA402_5", "FIBA403_5", "FI09_6", "FIBA401_6", "FIBA402_6", "FIBA403_6", "FI09_7", "FIBA401_7",
"FIBA402_7", "FIBA403_7", "FI09_8", "FIBA401_8", "FIBA402_8", "FIBA403_8", "FI09_9", "FIBA401_9", "FIBA402_9", "FIBA403_9", "FIBC201",
"ISZAKV_VK", "FOGLALKKOD_1NE", "FOGLALKKOD_2NE", "FOGLALKKOD_3NE", "FOGLALKNEV_1NE", "FOGLALKNEV_2NE", "FOGLALKNEV_3NE",
"FIBC220" ]].values.tolist())

cur.execute("commit")


# In[8]:


cur.close()


# In[ ]:




