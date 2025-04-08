import pandas as pd
import os
import oracledb
import getpass
import numpy as np
import datetime

def actual_time():
    f_now = datetime.datetime.now().strftime("%Y.%m.%d. %H:%M:%S")
    return f_now

def makeInsert(number):
    valuesText = ""
    for row in range(number):
        valuesText = valuesText + ":" + str(row + 1) + ","

    valuesText = valuesText[:-1]

    return valuesText

pd.options.mode.chained_assignment = None

username = getpass.getuser()
password = "Masodikpozicio1989" #getpass.getpass(f"Kérlek, add meg a(z) {username} felhasználói nevedhez tartozó jelszót: ")

database = oracledb.makedsn(host = "tesztdb.ksh.hu", port = "1522", service_name = "tesztdb.ksh.hu")
conn = oracledb.connect(user = username, password = password, dsn = database)
cur = conn.cursor()

TEV = "2024"
MHO = "11"
OSAP = "2588"
EXP_DATE = actual_time()

#Fürdőhelyek regisztrációs adatainak beolvasása Excel állományból
#regFurdok = pd.read_excel(io = os.path.abspath(os.getcwd() + "\Excel_Files\Regisztrációs_adatok_KSH_attrakcio_kozfurdo_természetesfurdohely_202406_08ho.xlsx"), sheet_name = "regisztráció", header = 0)
regFurdok = pd.read_excel(io = os.path.abspath(os.getcwd() + "\Excel_Files\Furdok_202411.xlsx"), sheet_name = "regisztráció", header = 0)
#regFurdok.drop(regFurdok.tail(2).index, inplace = True) #csak júniusra és júliusra kell törölni, mert augusztusban lett két új fürdőhely
print(f"A fürdőhelyek regisztrációs adatait tartalmazó adatkeret sor- és oszlopszámai : {regFurdok.shape}")

pd.set_option('display.precision', 0)
regFurdok["letrehozva"] = regFurdok["letrehozva"].astype("datetime64[ns]")
#print(regFurdok["letrehozva"].dtypes)
#print(regFurdok.loc[:]["letrehozva"])
regFurdok = regFurdok.replace({pd.NaT: None}).replace({"NaT": None}).replace({np.NaN: None})

regFurdok.insert(loc = 0, column = "MHO", value = MHO)
regFurdok.insert(loc = 0, column = "TEV", value = TEV)

values = makeInsert(85)

outputName = "GOA24.W_VK_2588_REG_V24H9_V_V00"
attributesForInsert = """TEV, MHO, szolgaltatasi_hely_nev, szolgaltatasi_hely_regisztracios_szam, foszolgaltatas, 
szolgaltatas_tipusok, statusz, letrehozva, szolgaltatasi_hely_iranyitoszam, szolgaltatasi_hely_telepules, 
szolgaltatasi_hely_megye, szolgaltatasi_hely_kiemelt_terseg, szolgaltatasi_hely_kozterulet_neve, 
szolgaltatasi_hely_kozterulet_jellege, szolgaltatasi_hely_hazszam, szolgaltato_nev, szolgaltato_adoszam, 
szolgaltato_vallalkozas_tipus, szolgaltato_statisztikai_tevekenyseg, szolgaltato_iranyitoszam, 
szolgaltato_telepules, arbevetel_ev, arbevetel_osszeg, arbevetel, altalanos_beszeltnyelvek, 
altalanos_feliratoknyelvei, altalanos_helyszinjellege, altalanos_atlagostoltottido_hour, 
altalanos_atlagostoltottido_minute, altalanos_atlagostoltottido_second, altalanos_atlagostoltottido_nano, 
altalanos_latogatokszamarawifi, altalanos_ajandekboltshowvan, altalanos_mobiltelefonosappvan, 
altalanos_turisztikaiinformaciospontvan, altalanos_kotelezoidopontotfoglalni, 
altalanos_szemelyesfoglalaslehetosegek, altalanos_nyitvatartasszezonalitasa, altalanos_vonzeronyitvavan, 
akadalymentesseg_lift, akadalymentesseg_wc, akadalymentesseg_fizikaiakadalymentesites, 
akadalymentesseg_bejaratmegkozelitheto, akadalymentesseg_latasserultekszamara, 
akadalymentesseg_hallasserultekszamara, akadalymentesseg_kiseroszemelyzetrendelkezesreall, gazdasagi_utalvanyok, 
gazdasagi_szepkartyak, gazdasagi_vanbankkartya, gazdasagi_fizetoeszkozok, gazdasagi_viszonteladoiertekesites,  
gazdasagi_jutalekosfizetesirendszer, infrastruktura_latogatowc, infrastruktura_ruhatar, 
infrastruktura_csomagmegorzo, infrastruktura_kerekpartarolo, infrastruktura_parkolo, infrastruktura_buszparkolodb, 
infrastruktura_szemelygepkocsiparkolodb, infrastruktura_elektromosautotoltes, furdokozfurdo_kategoria, 
furdoterulete, zoldteruletnagysaga, elmenyelemekszamaosszesen, medencekszamaosszesen, medencekvizfeluleteosszesen,  
furdomegengedhetonapilegnagyobbterhelese, furdobeepitettosszesvizforgatasikapacitasa,  
furdomegengedettegyidejulegnagyobbterhelese, furdoknemzetitanusitovedjegyevelrendelkezik, 
furdonekszerzodeseskapcsolataegeszsegpenztarral, furdoegysegek, beautyszolgaltatasok, csaladbaratszolgaltatasok, 
egeszsegmegorzoszolgaltatasok, maxbefogadokepesseg, partszakashossza, kekhullamminosites, zuhanylehetosegek,  
mozgaskorlatozottbetudjutniavizbe, vizimentoszolgalat, vizeskapcsolatosuszoda, lehetkolcsonozni, 
kolcsonzesilehetosegek, csaladbaratszolgaltatasok2""" 


output_insert_sql = "INSERT INTO " + outputName + "(" + attributesForInsert + ") VALUES(" + values + ")"
cur.executemany(output_insert_sql, regFurdok[["TEV", "MHO", "szolgaltatasi_hely_nev", "szolgaltatasi_hely_regisztracios_szam", 
                                              "foszolgaltatas", "szolgaltatas_tipusok", "statusz", "letrehozva", 
                                              "szolgaltatasi_hely_iranyitoszam", "szolgaltatasi_hely_telepules", 
                                              "szolgaltatasi_hely_megye", "szolgaltatasi_hely_kiemelt_terseg", 
                                              "szolgaltatasi_hely_kozterulet_neve", "szolgaltatasi_hely_kozterulet_jellege", 
                                              "szolgaltatasi_hely_hazszam", "szolgaltato_nev", "szolgaltato_adoszam", 
                                              "szolgaltato_vallalkozas_tipus", "szolgaltato_statisztikai_tevekenyseg", 
                                              "szolgaltato_iranyitoszam", "szolgaltato_telepules", "arbevetel_ev", 
                                              "arbevetel_osszeg", "arbevetel", "altalanos_beszeltnyelvek", 
                                              "altalanos_feliratoknyelvei", "altalanos_helyszinjellege", 
                                              "altalanos_atlagostoltottido_hour", "altalanos_atlagostoltottido_minute", 
                                              "altalanos_atlagostoltottido_second", 
                                              "altalanos_atlagostoltottido_nano", "altalanos_latogatokszamarawifi", 
                                              "altalanos_ajandekboltshowvan", "altalanos_mobiltelefonosappvan", 
                                              "altalanos_turisztikaiinformaciospontvan", "altalanos_kotelezoidopontotfoglalni", 
                                              "altalanos_szemelyesfoglalaslehetosegek", "altalanos_nyitvatartasszezonalitasa", 
                                              "altalanos_vonzeronyitvavan", "akadalymentesseg_lift", "akadalymentesseg_wc", 
                                              "akadalymentesseg_fizikaiakadalymentesites", "akadalymentesseg_bejaratmegkozelitheto", 
                                              "akadalymentesseg_latasserultekszamara", "akadalymentesseg_hallasserultekszamara", 
                                              "akadalymentesseg_kiseroszemelyzetrendelkezesreall", "gazdasagi_utalvanyok", 
                                              "gazdasagi_szepkartyak", "gazdasagi_vanbankkartya", "gazdasagi_fizetoeszkozok", 
                                              "gazdasagi_viszonteladoiertekesites", "gazdasagi_jutalekosfizetesirendszer", 
                                              "infrastruktura_latogatowc", "infrastruktura_ruhatar", "infrastruktura_csomagmegorzo", 
                                              "infrastruktura_kerekpartarolo", "infrastruktura_parkolo", "infrastruktura_buszparkolodb", 
                                              "infrastruktura_szemelygepkocsiparkolodb", "infrastruktura_elektromosautotoltes", 
                                              "furdokozfurdo_kategoria", "furdoterulete", "zoldteruletnagysaga", "elmenyelemekszamaosszesen", 
                                              "medencekszamaosszesen", "medencekvizfeluleteosszesen", "furdomegengedhetonapilegnagyobbterhelese", 
                                              "furdobeepitettosszesvizforgatasikapacitasa", "furdomegengedettegyidejulegnagyobbterhelese", 
                                              "furdoknemzetitanusitovedjegyevelrendelkezik", "furdonekszerzodeseskapcsolataegeszsegpenztarral", 
                                              "furdoegysegek", "beautyszolgaltatasok", "csaladbaratszolgaltatasok", 
                                              "egeszsegmegorzoszolgaltatasok", "maxbefogadokepesseg", "partszakashossza", "kekhullamminosites", 
                                              "zuhanylehetosegek", "mozgaskorlatozottbetudjutniavizbe", "vizimentoszolgalat", 
                                              "vizeskapcsolatosuszoda", "lehetkolcsonozni", "kolcsonzesilehetosegek", "csaladbaratszolgaltatasok2" ]].values.tolist())

cur.execute("commit")

cur.close()