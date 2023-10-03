from osgeo import ogr
from maquinas import maquinas
import shutil
import os
from datetime import datetime
from pprint import pprint

'''
Etapa atual, estou trabalhando para sincronizar uma segunda camada durante a migração 
'''


DIR_SQLITE = r"C:\python_projects\mapa_vant\teste_sqlite\mapa_vant_pilotos_dev.sqlite"
PG_HOST = None
DB_NAME = None
DB_USER = None
DB_PASSWORD = None


def copiar():
    ds_pg = ogr.Open("PG: host={} dbname={} user={} password={}".format(PG_HOST, DB_NAME, DB_USER, DB_PASSWORD), 1)
    ds_sqlite = sqlite_connect(DIR_SQLITE)
    lyr_sqlite = ds_sqlite.GetLayer('talhoeswgs84')
    lyr_pg = ds_pg.GetLayer('mapa_vant_dev.vw_talhoes_wgs84')
    # lyr_pg = ds_pg.ExecuteSQL('SELECT * FROM mapa_vant_dev.vw_talhoes_wgs84')
    ds_sqlite.CopyLayer(lyr_pg, 'teste_talhoes', ['OVERWRITE=YES'])


def update_talhoes_old():
    ds_pg = ogr.Open("PG: host={} dbname={} user={} password={}".format(PG_HOST, DB_NAME, DB_USER, DB_PASSWORD), 1)
    ds_sqlite = sqlite_connect(DIR_SQLITE)
    lyr_pg_talhoes = ds_pg.GetLayer('mapa_vant_dev.vw_talhoes_wgs84')
    lyr_sqlite_talhoes = ds_sqlite.GetLayer('talhoeswgs84')

    for feature_pg in lyr_pg_talhoes:
        ft_sqlite = lyr_sqlite_talhoes.GetFeature(feature_pg.GetFID())

        if not ft_sqlite:
            print("ID novo")
            print(f"Feature ID:{feature_pg.GetField('chave')} inserida na base dos pilotos")
            # lyr_sqlite_talhoes.CreateFeature(feature_pg)
        else:
            print("ID existente")
            print(f"Feature ID:{feature_pg.GetField('chave')} atualizada na base dos pilotos")
            # lyr_sqlite_talhoes.DeleteFeature(feature_pg.GetFID())
            # lyr_sqlite_talhoes.CreateFeature(feature_pg)
    lyr_pg_talhoes.ResetReading()

    # for feature_sqlite in lyr_sqlite_talhoes:
    #     ft_pg = lyr_pg_talhoes.GetFeature(feature_sqlite.GetFID())
    #     if not ft_pg:
    #         print("ID depreciado")
    #         print(f"Feature ID:{feature_sqlite.GetField('chave'), feature_sqlite.GetFID()} deletada da base dos pilotos")
    #         # lyr_sqlite_talhoes.DeleteFeature(feature_sqlite.GetFID())


def sqlite_connect(db_path):
    if os.path.exists(db_path):
        ds_sqlite = ogr.Open(db_path, 1)
        return ds_sqlite
    return None

'''
Método para a camada de talhões agir dinamicamente em relação a eventuais planos de voos que cubram esses talhões.
É feita uma iteração sobre as duas camadas de um mesmo banco para excluir ou destacar áreas sobrepostas.
'''
def select_within_features():
    ds_sqlite = sqlite_connect(DIR_SQLITE)
    lyr_plan_voo = ds_sqlite.GetLayer('plan_voo')
    lyr_talhoes = ds_sqlite.GetLayer('talhoeswgs84')
    areas_sobrevoadas = {}

    for ft_plan_voo in lyr_plan_voo:
        geom_ft_plan_voo = ft_plan_voo.GetGeometryRef()
        field_id = ft_plan_voo.GetFID()
        ano, mes, dia, *args = [str(x).zfill(2) if x else 'x' for x in ft_plan_voo.GetFieldAsDateTime('fim_voo')]
        data_voo = '/'.join([dia, mes, ano]) if 'x' not in [dia, mes, ano] else 'sem data'
        tipo_voo = ft_plan_voo.GetField('tipo_voo')
        for ft_talhoes in lyr_talhoes:
            geom_ft_talhoes = ft_talhoes.GetGeometryRef()
            chave = str(ft_talhoes.GetField('chave'))
            if geom_ft_plan_voo.Contains(geom_ft_talhoes):
                if field_id not in areas_sobrevoadas:
                    areas_sobrevoadas.update({field_id: [[chave, data_voo, tipo_voo]]})
                else:
                    areas_sobrevoadas[field_id].append([chave, data_voo, tipo_voo])
        lyr_talhoes.ResetReading()
    del ds_sqlite
    return areas_sobrevoadas

# Insere áreas sobrepostas em outra tabela de histórico
def gravar_voos(voos):
    ds_sqlite = sqlite_connect(DIR_SQLITE)
    query = '''
        INSERT INTO historico_voos (id_voo, chave, dt_voo, tipo_voo)
        VALUES ({}, {}, '{}', '{}')
    '''
    for id_voo, features in voos.items():
        print(id_voo)
        if len(features) > 1:
            for feature in features:
                print(id_voo, feature)
                ds_sqlite.ExecuteSQL(query.format(
                    id_voo,
                    feature[0],
                    feature[1],
                    feature[2]
                ))
        else:
            print(id_voo, features[0])
            ds_sqlite.ExecuteSQL(query.format(
                id_voo,
                features[0][0],
                features[0][1],
                features[0][2]
            ))
        print("*************************************")
    del ds_sqlite

# Método para resolver problemas com datas
def convert_str_to_date():
    ds_sqlite = sqlite_connect(DIR_SQLITE)
    lyr_talhoes = ds_sqlite.GetLayer('talhoeswgs84')
    dt = lyr_talhoes[0].GetFieldAsDateTime('dt_plantio')
    ano, mes, dia, *args = [str(x).zfill(2) for x in dt]
    print('/'.join([dia, mes, ano]))
    # print(datetime.strptime(dt, "%Y/%m/%d"))

# Método para realizar as mudanças nas camadas
def update_talhoes():
    ds_pg = ogr.Open("PG: host={} dbname={} user={} password={}".format(PG_HOST, DB_NAME, DB_USER, DB_PASSWORD), 1)
    ds_sqlite = sqlite_connect(DIR_SQLITE)
    lyr_pg_talhoes = ds_pg.GetLayer('mapa_vant_dev.vw_talhoes_wgs84')
    lyr_sqlite_talhoes = ds_sqlite.GetLayer('talhoeswgs84')
    chaves_sqlite = [x.GetField('chave') for x in lyr_sqlite_talhoes]
    chaves_pg = [x.GetField('chave') for x in lyr_pg_talhoes]

    insert_query = '''
        INSERT INTO talhoeswgs84 (chave, fazenda, talhao, area, projetos, dt_plantio, data_corte, estagio, de_varied, 
        tipo_reforma, dias_colheita, pos_falha, pos_plantio, status_falhas, simb, GEOMETRY)
        VALUES ({}, {}, {}, {}, '{}', {}, {}, '{}', '{}', '{}', {}, {}, {}, {}, {}, '{}', 
        CastToMultiPolygon(ST_GeomFromWKB('{}', 4326)))
    '''

    update_query = '''
        UPDATE talhoeswgs84 SET chave = {}, fazenda = {}, talhao = {}, area = {}, projetos = '{}', dt_plantio = {},
        data_corte = {}, estagio = '{}', de_varied = '{}', tipo_reforma = '{}', dias_colheita = {}, pos_falha = {}, 
        pos_plantio = {}, status_falhas = {}, simb = '{}', GEOMETRY = CastToMultiPolygon(ST_GeomFromWKB('{}', 4326))
        WHERE chave = {} 
    '''

    print(f"Layer pg: {len(chaves_pg)} feições")
    print(f"Layer sqlite: {len(chaves_sqlite)} feições")
    print(f"Diferença: {len(chaves_pg) - len(chaves_sqlite)} feições")

    lyr_pg_talhoes.ResetReading()

    for feature_pg in lyr_pg_talhoes:

        chave = feature_pg.GetField("chave")
        fazenda = feature_pg.GetField("fazenda")
        talhao = feature_pg.GetField("talhao")
        area = feature_pg.GetField("area")
        projetos = feature_pg.GetField("projetos")
        geometria = feature_pg.GetGeomFieldRef("geometria")
        data_plantio = feature_pg.GetField("dt_plantio") or "SEM DATA"
        data_corte = feature_pg.GetField("data_corte") or "SEM DATA"
        estagio = feature_pg.GetField("estagio")
        variedade = feature_pg.GetField("de_varied")
        tipo_reforma = feature_pg.GetField("tipo_reforma")
        dias_colheita = feature_pg.GetField("dias_colheita")
        pos_falha = feature_pg.GetField("pos_falha") or 0
        pos_plantio = feature_pg.GetField("pos_plantio") or 0
        status_falhas = feature_pg.GetField("status_falhas")
        simbologia = feature_pg.GetField("simb") or "SEM SIMB"

        if chave not in chaves_sqlite:
            ds_sqlite.ExecuteSQL(insert_query.format(chave, fazenda, talhao, area, projetos, data_plantio,
                                                     data_corte, estagio, variedade, tipo_reforma, dias_colheita,
                                                     pos_falha, pos_plantio, status_falhas, simbologia, geometria))
            # print(chave, fazenda, talhao, area, projetos, data_plantio, data_corte, estagio, variedade, tipo_reforma,
            #       dias_colheita, pos_falha, pos_plantio, status_falhas, simbologia, geometria)

            print(f"Chave {chave} adicionada!")

        else:
            ds_sqlite.ExecuteSQL(update_query.format(chave, fazenda, talhao, area, projetos, data_plantio,
                                                     data_corte, estagio, variedade, tipo_reforma, dias_colheita,
                                                     pos_falha, pos_plantio, status_falhas, simbologia, geometria, chave))
            # print(chave, fazenda, talhao, area, projetos, data_plantio, data_corte, estagio, variedade, tipo_reforma,
            #       dias_colheita, pos_falha, pos_plantio, status_falhas, simbologia, geometria)
            print(f"Chave {chave} atualizada!")

    print("Atualização concluída!!!")
    print(f"Layer pg: {len(lyr_pg_talhoes)} feições")
    print(f"Layer sqlite: {len(lyr_sqlite_talhoes)} feições")
    print(f"Diferença: {len(lyr_pg_talhoes) - len(lyr_sqlite_talhoes)} feições")

    del ds_sqlite, ds_pg


update_talhoes()
