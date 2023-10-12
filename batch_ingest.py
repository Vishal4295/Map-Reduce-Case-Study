import happybase

connection = happybase.Connection('localhost', port=9090 , autoconnect=False)

def open_connection():
    connection.open()

def close_connection():
    connection.close()


def get_table():
    open_connection()
    table = connection.table('nytc')
    close_connection()
    return table

def batch_insert_data(filename,start_count):
    print("Starting batch insert of Trip Data for file :", filename)
    file = open(filename, "r")
    table = get_table()
    open_connection()
   
    row_key = start_count
    i = 0
    with table.batch(batch_size=1000) as b:
        for line in file:
            if i!=0:    
                cols = line.strip().split(",")
                b.put( row_key, { "trip_details:vendorid": cols[0], "trip_details:tpep_pickup_datetime": cols[1],   "trip_details:tpep_dropoff_datetime": cols[2], "trip_details:passenger_count": cols[3], "trip_details:trip_distance": cols[4], "trip_details:ratecodeid": cols[5], "trip_details:store_and_fwd_flag": cols[6], "trip_details:pulocationid": cols[7], "trip_details:dolocationid": cols[8], "invoice_details:payment_type": cols[9], "invoice_details:fare_amount": cols[10], "invoice_details:extra": cols[11], "invoice_details:mta_tax": cols[12], "invoice_details:tip_amount": cols[13], "invoice_details:tolls_amount": cols[14], "invoice_details:improvement_surcharge": cols[15], "invoice_details:total_amount": cols[16], "invoice_details:congestion_surcharge": cols[17], "invoice_details:airport_fee": cols[18] })

                row_key = str(int(row_key)+1) 

            i+=1        
    file.close()
    close_connection()
    return row_key   


rows1 = batch_insert_data('yellow_tripdata_2017-03.csv', '18880589')
rows2 = batch_insert_data('yellow_tripdata_2017-04.csv', rows1)
