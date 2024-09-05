#akses data ke database
#cara pakai library psycopg2, kalau mau akses data/ lakukan query ke postgresql pake library ini

import psycopg2 as pg # library untuk akses database, ini library postgree

# Mendefinisikan host, port, database, user, dan password untuk koneksi ke database.
host = "35.222.95.127"
port = "5432"
database = "postgres"
user = "postgres"
password = "P@ssw0rd"
dsn = "dbname=" + database + " user=" + user + " host=" + host + " port=" + port + " password=" + password # Membuat sebuah dsn (Data Source Name) yang digunakan untuk menghubungkan ke database.

conn = pg.connect(dbname=database,host=host, port=port, user=user,password=password) # Menghubungkan ke database dengan menggunakan parameter yang telah didefinisikan sebelumnya

# fungsi untuk baca dari database
def read_from_db(conn, query): # Fungsi ini akan membuat objek cursor dengan menggunakan koneksi "conn" dan melakukan eksekusi query.
    cur = conn.cursor() # membuka cursor, cur untuk mengarahkan ke db supaya bisa jalanin query, conn.cursor adalah library psycopg
    cur.execute(query) # mengeksekusi query pada database yang telah dihubungkan
    data = cur.fetchall() # mengambil seluruh data hasil query atau mengambil hasil query
    cur.close() # setiap bikin cursor langsung di close
    return data # data hasil query akan dikembalikan sebagai output dari fungsi

#fungsi untuk eksekusi query
def exec_db(conn, query, data=None): # fungsi exec_db dipanggil dengan parameter conn (koneksi ke database), query (query SQL), dan data (data yang akan dimasukkan ke dalam query).
    cur = conn.cursor() # membuka cursor
    cur.execute(query, data) # mengeksekusi query pada database yang telah dihubungkan dengan data yang diberikan
    conn.commit() #harus di commit untuk menyimpan perubahan atau supaya mendokumentasikan di DB,  meng-commit perubahan yang terjadi pada database (jika ada)
    cur.close() # cursor ditutup

# fungsi untuk baca data customer dari tabel olist_customers_dataset_csv
def read_customer(): # Fungsi ini akan membuat variabel "query" yang berisi perintah SQL dibawah
    query = """
            select customer_unique_id,customer_id, customer_zip_code_prefix, customer_city, customer_state 
            from olist_customers_dataset_csv
            limit 100
    """
    data=read_from_db(conn, query) # membaca data dari database dengan memanggil fungsi `read_from_db` yang telah didefinisikan sebelumnya.
    allrows = [] # variabel `allrows` didefinisikan untuk menampung semua data pelanggan yang telah diproses.
    for row in data: # sebuah loop `for` akan berjalan memproses setiap baris data pelanggan yang ditemukan.
        elmt={} # Variabel `elmt` akan digunakan untuk menampung setiap baris data pelanggan dalam bentuk sebuah dictionary
        elmt["customer_id"] = row[1]
        elmt["customer_unique_id"] = row[0]
        elmt["customer_zip_code_prefix"] = row[2]
        elmt["customer_city"] = row[3]
        elmt["customer_state"] = row[4]
        allrows.append(elmt) # Dictionary "elmt" tadi akan dimasukkan ke dalam list "allrows" dengan menggunakan "append()"
    return allrows # fungsi ini akan mengembalikan nilai `allrows` berisi semua data pelanggan yang telah diproses.

def write_customer(): # digunakan untuk menambahkan data ke dalam tabel "olist_customers_dataset_csv".
    query = """
        insert into olist_customers_dataset_csv(customer_unique_id,customer_id, customer_zip_code_prefix, customer_city, customer_state)
        values(%s,%s,%s,%s,%s)
    """ # Variabel "query" berisi query SQL untuk melakukan insert data pada tabel "olist_customers_dataset_csv" dengan parameter yang disesuaikan.
    exec_db(conn, query, ('1234567890', '888889', '15345', 'Surabaya', 'SU') ) # fungsi "exec_db" akan dipanggil untuk mengeksekusi query SQL, dengan parameter koneksi "conn" dan data pelanggan baru.
    print("berhasil insert data") # Mengeluarkan pesan "berhasil insert data" setelah query dijalankan dengan sukses.

def update_customer(): # Fungsi "update_customer" digunakan untuk mengupdate/mengubah data di dalam tabel "olist_customers_dataset_csv".
    query = """
        update olist_customers_dataset_csv
        set customer_city='Medan'
        where customer_id='888889'
    """ # Variabel "query" akan memuat query SQL untuk memperbarui data pelanggan pada tabel "olist_customers_dataset_csv"
    exec_db(conn, query) #  Kemudian, fungsi "exec_db" akan dipanggil untuk mengeksekusi query SQL, dengan parameter koneksi "conn" dan query SQL yang dihasilkan.
    print("berhasil update data")

#untuk memanggil atau menampilkan hasil query customer, coba perintah dibawah ini (data....., print.....)
#data = read_customer()
#print(data)

write_customer()
#update_customer()