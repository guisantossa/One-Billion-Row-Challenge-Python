from using_python import processar_temperaturas
from using_dask import create_dask_df
from using_duckdb import create_duckdb
from using_pandas import create_df_with_pandas
from using_polars import create_polars_df
from create_measurements import main_externo
import time
import warnings


if __name__ == "__main__":
    
    #warnings.filterwarnings("ignore")

    
    try:
        num_rows_to_create = int(input("Digite o número de Linhas a serem processadas: "))
        main_externo(num_rows_to_create)
        print("Arquivo de teste finalizado.")
        path_do_csv = "data/measurements.txt"
        
        print("Iniciando o processamento do arquivo com Python")
        start_time = time.time()  # Tempo de início
        resultados = processar_temperaturas(path_do_csv)
        end_time = time.time()  # Tempo de término
        print(f"\nProcessamento concluído em {end_time - start_time:.2f} segundos.")

        print("Iniciando o processamento do arquivo com dask")
        start_time = time.time()
        df = create_dask_df()
        # O cálculo real e a ordenação são feitos aqui
        result_df = df.compute().sort_values("station")
        took = time.time() - start_time
        print(f"Dask Took: {took:.2f} sec")

        print("Iniciando o processamento do arquivo com duckdb")
        start_time = time.time()
        create_duckdb()
        took = time.time() - start_time
        print(f"Duckdb Took: {took:.2f} sec")

        print("Iniciando o processamento do arquivo com pandas")
        start_time = time.time()
        total_linhas = 1_000_000_000  # Total de linhas conhecido
        chunksize = 100_000_000  # Define o tamanho do chunk
        df = create_df_with_pandas(path_do_csv, total_linhas, chunksize)
        took = time.time() - start_time

        
        print(f"Processing took: {took:.2f} sec")
        print("Iniciando o processamento do arquivo com Polars")
        start_time = time.time()
        df = create_polars_df()
        took = time.time() - start_time
        
        print(f"Polars Took: {took:.2f} sec")
    except ValueError:
        print("Você não digitou um número!")
    except KeyboardInterrupt:
        print("Interrompido pelo usuário")