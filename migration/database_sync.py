from osgeo import ogr
from maquinas import maquinas
import shutil
import os

# A conexão com o banco central é estabelecida no construct da classe, o parâmetro '1' significa permissão para editar
class Synchronize:
    def __init__(self):
        self.db_pg = ogr.Open("PG: host='host' dbname='dbname' user='username' password='password'", 1)
        self.maquinas = None
        self.server_name = None
        self.pasta_bases_pilotos = None
        self.data_sources = {}
        self.insert_or_update = []

    # Método para estabelecer conexão com bancos sqlite
    @staticmethod
    def sqlite_connect(db_path):
        if os.path.exists(db_path):
            ds_sqlite = ogr.Open(db_path, 1)
            return ds_sqlite
        return None

    # Método para comparar os bancos e armazenar as diferenças em um dicionário
    def compare_ogr(self):
        # Itera sobre as máquinas cadastradas no arquivo 'máquinas.py'
        for idx, maquina in enumerate(self.maquinas):
            ds_sqlite_piloto = r"\\{}\c$\python_projects\mapa_vant\mapa_vant_pilotos_dev{}.sqlite"
            ds_sqlite_servidor = r"\\{}\c$\python_projects\mapa_vant\pilotos\mapa_vant_pilotos_dev{}.sqlite"
            try:
                # Valida se é possível abrir conexão e copia o banco de cada piloto para diretório local
                shutil.copy(ds_sqlite_piloto.format(maquina, ''),
                            ds_sqlite_servidor.format(self.server_name, '_' + str(idx)))
                self.maquinas[maquina] = {'conn': True}
                print(f"Conectado à {maquina}")
            except:
                print(f"Não foi possível conectar à máquina: {maquina}")
                self.maquinas[maquina] = {'conn': False}

        # Separa o nome de cada banco em uma lista
        sqlites = [x for x in os.listdir(self.pasta_bases_pilotos) if x.endswith('sqlite')]
        print(sqlites)

        '''
         Iterando sobre cada banco, são abertas conexões entre esse e o central e são extraídas as camadas de interesse
         Um dicionário é preenchido se houver features diferentes entre os bancos, há também a classificação dessas
         diferenças entre "inserir", "deletar" e "atualizar".
         Se a mesma feature existir nos bancos, mas houver divergência em alguma coluna, é setada a atualização, caso
         não houver no banco dos pilotos, inserir, caso contrário, deletar
         O banco dos pilotos contém um Trigger que alimenta uma tabela com um histórico para áreas deletas, é com base
         nessa tabela que a exclusão é feita.
         '''
        for sqlite in sqlites:

            caminho_sqlite = os.path.join(self.pasta_bases_pilotos, sqlite)
            ds_sqlite_piloto = self.sqlite_connect(caminho_sqlite)

            if not ds_sqlite_piloto:
                print("Não foi possível conectar ao banco")
                continue

            lyr_pg = self.db_pg.GetLayer("mapa_vant_dev.plan_voo")
            lyr_sqlite = ds_sqlite_piloto.GetLayer("plan_voo")
            lyr_del_sqlite = ds_sqlite_piloto.GetLayer("plan_voo_del")
            features_novas = {caminho_sqlite: {}}

            if lyr_del_sqlite:
                for feat in lyr_del_sqlite:
                    if feat.GetField("status") == 0:
                        features_novas[caminho_sqlite].update({'deletar': feat})

            for feature_sqlite in lyr_sqlite:
                feature_pg = lyr_pg.GetFeature(feature_sqlite.GetFID())
                if not feature_pg:
                    features_novas[caminho_sqlite].update({'inserir': feature_sqlite})
                    continue
                jsqlite = feature_sqlite.ExportToJson()
                jpg = feature_pg.ExportToJson()
                if jpg != jsqlite:
                    features_novas[caminho_sqlite].update({'atualizar': feature_sqlite})
            lyr_sqlite.ResetReading()

            if not features_novas[caminho_sqlite]:
                print(f"Não há nada para baixar do banco \"{sqlite}\".")
            if features_novas[caminho_sqlite]:
                self.insert_or_update.append(features_novas)
        print(features_novas)

    '''
    Método para inserir mudanças ocorridas nos bancos dos pilotos para o banco central, aqui o método praticamente não
    faz validações, já que ele será chamado apenas para inserir dados já validados, os dicionários dentro do dicionário
    alimentados pelo método de comparação
    '''
    def to_pg(self):
        lyr_pg = self.db_pg.GetLayer("mapa_vant_dev.plan_voo")
        for valor in self.insert_or_update:
            for caminho, status in valor.items():
                for acao, feat in status.items():
                    if acao == 'atualizar':
                        lyr_pg.DeleteFeature(feat.GetField('pkuid'))
                        lyr_pg.CreateFeature(feat)
                        print(f'Feature {feat.GetFID()} atualizada.')
                    elif acao == 'inserir':
                        lyr_pg.CreateFeature(feat)
                        print(f'Feature {feat.GetFID()} inserida.')
                    elif acao == 'deletar':
                        try:
                            lyr_pg.DeleteFeature(feat.GetField('pkuid'))
                            print(f"Feature {feat.GetField('pkuid')} deletada.")
                        except:
                            print("Não há essa feature no Postgres")

    '''
        Método para inserir atualizações do banco central no banco dos pilotos, o funcionamento é semelhante ao do
        método de inserção no Postgres, age conforme um dicionário com informações já validadas
        '''
    def to_sqlite(self, ds_path):
        ds_sqlite = self.sqlite_connect(ds_path)
        lyr_plan_voo_pg = self.db_pg.GetLayer("mapa_vant_dev.plan_voo")
        lyr_sqlite = ds_sqlite.GetLayer("plan_voo")
        lyr_sqlite_del = ds_sqlite.GetLayer("plan_voo_del")

        for feature in lyr_plan_voo_pg:
            feature_sqlite = lyr_sqlite.GetFeature(feature.GetFID())
            if not feature_sqlite:
                lyr_sqlite.CreateFeature(feature)
        lyr_plan_voo_pg.ResetReading()
        for feat in lyr_sqlite_del:
            query = r"UPDATE plan_voo_del SET status = '1' WHERE pkuid = {}"
            ds_sqlite.ExecuteSQL(query.format(feat.GetField("pkuid")))

    # Método principal, onde une os outros métodos para fazer a sincronização
    def sync(self):
        self.compare_ogr()

        # Se o dicionário estiver vazio, não há nada para atualizar
        if not self.insert_or_update:
            return print("Nada para atualizar")

        # Separa os endereços dos bancos
        bancos_desatualizados = [caminho for layer in self.insert_or_update for caminho, status in layer.items()]

        if not bancos_desatualizados:
            return print("Os bancos já estão sincronizados")

        # As alterações são passadas só o para último banco dos pilotos, após isso basta enviar uma cópia aos outros
        self.to_pg()
        ultimo_banco = bancos_desatualizados[-1]
        self.to_sqlite(ultimo_banco)

        ds_sqlite_servidor = r"\\{}\c$\python_projects\mapa_vant\mapa_vant_pilotos_dev.sqlite"

        # Estabelece conexão com o último banco colado em diretório local, já atualizado, e copia para os outros pilotos
        for maquina, conn in self.maquinas.items():
            if conn["conn"] is True:
                print(ds_sqlite_servidor.format(maquina))
                shutil.copy(ultimo_banco, ds_sqlite_servidor.format(maquina))


if __name__ == '__main__':
    sy = Synchronize()
    sy.server_name = 'DT220056'
    sy.maquinas = maquinas
    sy.pasta_bases_pilotos = r'C:\python_projects\mapa_vant\pilotos'
    sy.sync()

