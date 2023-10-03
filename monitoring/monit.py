import os
from time import sleep
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from connection import Db

'''
Inicialmente a ideia era disparar uma trigger através do monitoramento do diretório que recebe os arquivos de voo para
popular o banco com base em um arquivo com extensão '.idvoo'
Vimos que seria melhor criar essa trigger dentro do banco, uma vez que as migrações trabalham com CRUD's ao invés de
apenas copiar as camadas.
'''

'''
A ideia aqui agora é usar esse monitoramento para auxiliar em outras automatizações do setor, como planilhas e envio de
alertas por e-mail.
'''

class WatchdogHandlerAny(FileSystemEventHandler):
    last_idvoo = None

    def on_created(self, event):
        if not event.is_directory:
            path_splited = event.src_path.split('\\')
            arq_name = path_splited[-1]
            arq, extension, *args = arq_name.split('.')
            if extension == 'idvoo' and self.last_idvoo != arq:
                self.last_idvoo = arq
                p_path = self.processing_path(path_splited)
                self.upsert(arq, p_path)

    def on_modified(self, event):
        if not event.is_directory and event.event_type != 'DELETED':
            path_splited = event.src_path.split('\\')
            arq_name = path_splited[-1]
            arq, extension, *args = arq_name.split('.') or 'x' * 3
            if extension == 'idvoo' and self.last_idvoo != arq:
                self.last_idvoo = arq
                if self.search(arq):
                    print('Voo existente no banco de Dados')
                    return
                p_path = self.processing_path(path_splited)
                self.upsert(arq, p_path)

    @staticmethod
    def search(code):
        db = Db()
        conn = db.connect()
        conn.execute(f"SELECT * FROM mapa_vant_dev.info_voo WHERE id_voo = {code}")
        res = conn.fetchall()        if not res:
            return False
        return res

    @staticmethod
    def processing_path(_path):
        del _path[-1]
        _path[-2] = 'A PROCESSAR'
        fullpath = '\\'.join(_path)
        if os.path.exists(fullpath):
            print('Caminho existente')
            return False
        os.mkdir(fullpath)
        return fullpath

class FolderWatchDog:
    def __init__(self, handler, watch_path):
        self.handler = handler
        self.watch_path = watch_path
        self.observer = Observer()

    def start(self):
        self.observer.schedule(self.handler, path=self.watch_path, recursive=True)
        try:
            print(f'Starting to watch folder: {self.watch_path}')
            self.observer.start()
            while True:
                sleep(3)
        except Exception as e:
            print("Something went wrong...")
            print(e)
        finally:
            print("All done!")

    def stop(self):
        self.observer.stop()
        self.observer.join()


if __name__ == '__main__':
    to_watch = r'G:\Geotec - 2020\2023'
    w_dog = FolderWatchDog(handler=WatchdogHandlerAny(), watch_path=to_watch)
    w_dog.start()
'''dGVzdGU='''
