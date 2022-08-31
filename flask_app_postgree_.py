from flask import Flask
import psycopg2 as pg

from flask import request


#credential
host = "35.xxx.xx.xxx"
port = "5xxx"
database = "postgres"
user = "xxxxxxxx"
password = "xxxxxxxx"
dsn = "dbname=" + database + " user=" + user + " host=" + host + " port=" + port + " password=" + password

#conn = pg.connect(dsn)
conn = pg.connect(dbname=database,host=host, port=port, user=user,password=password)

app = Flask(__name__)

def read_from_db(conn, query):
    # fungsi untuk baca dari database
    cur = conn.cursor()
    cur.execute(query)
    data = cur.fetchall()
    cur.close()
    return data

def exec_db(conn, query, data=None):
    cur = conn.cursor()
    cur.execute(query, data)
    conn.commit()
    cur.close()

def read_customer():
    # fungsi untuk baca data customer
    query = """
            select customer_unique_id,customer_id, customer_zip_code_prefix, customer_city, customer_state 
            from olist_customers_dataset_csv
            limit 100
    """
    data=read_from_db(conn, query)    
    allrows = []
    for row in data:
        elmt={}
        elmt["customer_id"] = row[1]
        elmt["customer_unique_id"] = row[0]
        elmt["customer_zip_code_prefix"] = row[2]
        elmt["customer_city"] = row[3]
        elmt["customer_state"] = row[4]
        allrows.append(elmt)
    return allrows

def write_customer(data):
    query = """
        insert into olist_customers_dataset_csv(customer_unique_id, customer_id, customer_zip_code_prefix, customer_city, customer_state)
        values(%s,%s,%s,%s,%s)
    """
    exec_db(conn, query, data )

def update_customer(city, cust_id):
    query = f"""
        update olist_customers_dataset_csv
        set customer_city='{city}'
        where customer_id='{cust_id}'
    """
    exec_db(conn, query)

def delete_customer(cust_id):
    query = f"""
        delete from olist_customers_dataset_csv
        where customer_id='{cust_id}'
    """
    exec_db(conn, query)

@app.route('/customer', methods=['GET','POST','PUT', 'DELETE'])
def index():
    if request.method == "GET":
        data = read_customer()
        return { "alldata" : data } #return dalam bentuk json/dict karena dalam bentuk list

    elif request.method == "POST":
        body = request.get_json()
        data = ( body['customer_unique_id'], body['customer_id'], body['customer_zip_code_prefix'], body['customer_city'], body['customer_state'] ) #ini json/dict
        write_customer( data )

        return { "message" : "berhasil input data" }
    
    elif request.method == "PUT":
        body = request.get_json()

        update_customer(body["customer_city"], body["customer_id"] )

        return { "message" : f"berhasil update data : {body['customer_id']}" }
    
    elif request.method == "DELETE":
        body = request.get_json()
        delete_customer(body["customer_id"])

        return { "message" : f"berhasil delete data - {body['customer_id']}" }

    return { "message" : "not found" }


'''
@app.route('/customer_list', methods=['GET'])
def customer_list():
    data = read_customer()
    return { "alldata" : data }

@app.route('/customer_insert', methods=['POST'])
def customer_insert():
    body = request.get_json()
    data = ( body['customer_unique_id'], body['customer_id'], body['customer_zip_code_prefix'], body['customer_city'], body['customer_state'] )
    write_customer( data )

    return { "message" : "berhasil input data" }

@app.route('/customer_update', methods=['PUT'])
def customer_update():
    body = request.get_json()

    update_customer(body["customer_city"], body["customer_id"] )

    return { "message" : f"berhasil update data : {body['customer_id']}" }

'''

if __name__ == '__main__':
    app.run(host='0.0.0.0')
