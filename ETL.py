from msilib.schema import tables
import pandas as pd
from sqlalchemy import *
from sqlalchemy_utils import database_exists, create_database


class ETL:
    def __init__(self):

    

        self.conn = create_engine("mysql+pymysql://root:@localhost/filipao")
        if not database_exists(self.conn.url):
            create_database(self.conn.url)
            print(database_exists(self.conn.url))

        metadata_obj = MetaData()

        Table(
            'DIM_Produto', metadata_obj,
            Column('ID_produto', Integer, primary_key=True),
            Column('Cod_produto', Integer),
            Column('Nm_produto', VARCHAR(20)),
            Column('Cod_produto', Integer),
            Column('Secao', VARCHAR(45)),
            Column('Grupo', VARCHAR(45)),
            Column('Subgrupo', VARCHAR(45)),
            mysql_engine='InnoDB',
            keep_existing=True
        )

        Table(
            'DIM_lala', metadata_obj,
            Column('ID_produto', Integer, primary_key=True),
            Column('Cod_produto', Integer),
            Column('Nm_produto', VARCHAR(20)),
            Column('Cod_produto', Integer),
            Column('Secao', VARCHAR(45)),
            Column('Grupo', VARCHAR(45)),
            Column('Subgrupo', VARCHAR(45)),
            mysql_engine='InnoDB',
            keep_existing=True
        )





        metadata_obj.create_all(self.conn)


def main():
    ETL()


if __name__ == "__main__":
    main()
