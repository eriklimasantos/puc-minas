import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

URL = "https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv"


default_args = {
    'owner': "ERIK",
    "depends_on_past": False,
    'start_date': datetime(2022, 10, 12)
}

@dag(default_args=default_args, schedule='@once', catchup=False, tags=['Titanic'])
def trabalho2_dag1():
    @task
    def ingestao():
        NOME_DO_ARQUIVO = "/tmp/titanic.csv"
        df = pd.read_csv(URL, sep=';')
        df.to_csv(NOME_DO_ARQUIVO, index=False, sep=";")
        return NOME_DO_ARQUIVO

    @task
    def ind_passageiros(nome_do_arquivo):
        NOME_TABELA = "/tmp/passageiros_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        res1 = df.groupby(['Sex', 'Pclass']).agg({"PassengerId": "count"}).reset_index()
        res1.to_csv(NOME_TABELA, index=False, sep=";")
        return NOME_TABELA

    fim = EmptyOperator(task_id="fim")
    ing = ingestao()
    indicador = ind_passageiros(ing)



    @task
    def mean_passageiros(nome_do_arquivo):
        NOME_TABELA2 = "/tmp/passageiros_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        res2 = df.groupby(['Sex', 'Pclass']).agg({"Fare": "mean"}).reset_index()
        res2.to_csv(NOME_TABELA2, index=False, sep=";")
        return NOME_TABELA2

    fim2 = EmptyOperator(task_id="fim2")
    ing = ingestao()
    indicadormean = mean_passageiros(ing)

    @task
    def sum_SibSpParch(nome_do_arquivo):
        NOME_TABELA3 = "/tmp/passageiros_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        res3 = df.groupby(['Sex','Pclass'])['SibSp','Parch'].sum().reset_index()
        res3.to_csv(NOME_TABELA3, index=False, sep=";")
        return NOME_TABELA3

    fim3 = EmptyOperator(task_id="fim3")
    ing = ingestao()
    indicadorsum = sum_SibSpParch(ing)



    @task
    def export_csv(nome_do_arquivo):
        df = pd.read_csv(nome_do_arquivo, sep=";")
        res1 = df.groupby(['Sex', 'Pclass']).agg({"PassengerId": "count"}).reset_index()
        res2 = df.groupby(['Sex', 'Pclass']).agg({"Fare": "mean"}).reset_index()
        res3 = df.groupby(['Sex', 'Pclass'])['SibSp', 'Parch'].sum().reset_index()
        result = pd.concat([res1,res2,res3], axis=1)
        nome = "/tmp/tabela_unica.csv"
        result.to_csv(nome, index=False, sep=";")

    fim4 = EmptyOperator(task_id="fim4")

    triggerdag = TriggerDagRunOperator(
        task_id="trigga_dag2",
        trigger_dag_id= "trabalho2_dag2"
    )

    indicadototal = export_csv(ing)
    indicador >> fim >> indicadormean >> fim2 >> indicadorsum >> fim3 >> indicadototal >> fim4 >> triggerdag

execucao = trabalho2_dag1()