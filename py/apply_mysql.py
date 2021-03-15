import pandas as pd
import pymysql


def execute():

    def make_sql(list_column_value):
        whole_cols = ""
        for row_data in list_column_value:
            row_cols = "("
            for col in row_data:
                if type(col) == int:
                    col = str(col) + ","
                else:
                    col = "'" + col + "',"
                row_cols += col
            row_cols = row_cols + ")"
            row_cols = row_cols.replace(",)", "),")
            whole_cols += row_cols
        
        sql = "insert into grades " + column_name.upper() + "\n"
        sql = sql + "values \n"
        sql = sql + whole_cols

        return sql[:len(sql) - 1]

    def insert_data(sql):
        conn = pymysql.connect(host='localhost', user='etlers', password='wndyd', charset='utf8', database="chois") 
        
        cursor = conn.cursor() 
        cursor.execute(sql)        

        conn.commit()
        conn.close()

    df_grades = pd.read_csv("/home/etlers/airflow/dags/csv/test.csv")

    column_name = "("
    for cols in list(df_grades):
        column_name = column_name + cols + ","
    column_name = column_name + ")"
    column_name = column_name.replace(",)", ")")
    column_name

    sql = make_sql(df_grades.values.tolist())

    insert_data(sql)


if __name__ == "__main__":
    execute()